"""
Mar Menor ecological monitoring scraper.

Downloads data from four official sources, stores in SQLite,
and exports static CSVs for the public dashboard.

Sources:
  - html:   Monitoring HTML page (most recent values)
  - cdg:    CdG weekly PDF reports
  - imida:  IMIDA weekly PDF reports
  - aforos: Albujón flow/nutrients weekly PDFs
"""

import csv
import io
import logging
import os
import re
import sqlite3
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pdfplumber
import requests
from bs4 import BeautifulSoup

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "marmenor.db"
PDF_DIR  = BASE_DIR / "pdfs"
DATA_DIR = BASE_DIR / "datos"
SCHEMA   = BASE_DIR / "schema.sql"

PDF_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Spanish month abbreviation map ───────────────────────────────────────────

MONTHS_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

# Parameter keywords used to identify rows in PDFs
PARAM_KEYWORDS = {
    "temperatura":    "temperatura",
    "salinidad":      "salinidad",
    "clorofila":      "clorofila_ug_l",
    "oxígeno":        "oxigeno_mg_l",
    "oxigeno":        "oxigeno_mg_l",
    "turbidez":       "turbidez_ftu",
    "transparencia":  "transparencia_m",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def parse_float(s: str) -> Optional[float]:
    """Parse a float from Spanish-formatted strings.

    Handles decimal commas ('1,23' → 1.23), strips units and
    whitespace, returns None if the value cannot be parsed.
    """
    if not s:
        return None
    s = s.strip()
    # Replace Spanish decimal comma
    s = s.replace(",", ".")
    # Remove everything that is not a digit, dot, or minus sign
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def spanish_date_to_iso(day_month: str, year_hint: int) -> Optional[str]:
    """Convert 'DD mes' (e.g. '09 mar') to 'YYYY-MM-DD'.

    If the parsed month is more than 2 months ahead of the current
    month, it is assumed to belong to the previous year.
    """
    parts = day_month.strip().lower().split()
    if len(parts) != 2:
        return None
    day_str, month_str = parts
    month_num = MONTHS_ES.get(month_str[:3])
    if not month_num:
        return None
    try:
        day = int(day_str)
    except ValueError:
        return None

    year = year_hint
    current_month = date.today().month
    if month_num > current_month + 2:
        year -= 1

    try:
        return date(year, month_num, day).isoformat()
    except ValueError:
        return None


def get_db() -> sqlite3.Connection:
    """Open SQLite connection and initialize schema if needed."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    with open(SCHEMA) as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def log_result(conn: sqlite3.Connection, source: str, new_records: int, error: Optional[str]):
    """Write one row to meta_downloads for the current run."""
    conn.execute(
        "INSERT INTO meta_downloads (source, download_date, new_records, error) VALUES (?, ?, ?, ?)",
        (source, date.today().isoformat(), new_records, error),
    )
    conn.commit()


# ─── Idempotency: seed DB from existing CSVs ──────────────────────────────────

def seed_from_csv(conn: sqlite3.Connection):
    """Load existing CSV data into SQLite at startup.

    This makes scraper.py idempotent across stateless CI runs:
    the CSVs in the repo are the canonical record of what has
    already been processed.
    """
    params_csv = DATA_DIR / "parametros.csv"
    aforos_csv = DATA_DIR / "aforos.csv"

    if params_csv.exists():
        with open(params_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                conn.execute(
                    """INSERT OR IGNORE INTO parametros_laguna
                       (fecha, fuente, temperatura, salinidad, clorofila_ug_l,
                        oxigeno_mg_l, turbidez_ftu, transparencia_m)
                       VALUES (:fecha, :fuente, :temperatura, :salinidad,
                               :clorofila_ug_l, :oxigeno_mg_l,
                               :turbidez_ftu, :transparencia_m)""",
                    row,
                )
        conn.commit()
        log.info("Seeded parametros_laguna from %s", params_csv)

    if aforos_csv.exists():
        with open(aforos_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                conn.execute(
                    """INSERT OR IGNORE INTO aforos_albujon
                       (fecha, caudal_l_s, nitratos_mg_l)
                       VALUES (:fecha, :caudal_l_s, :nitratos_mg_l)""",
                    row,
                )
        conn.commit()
        log.info("Seeded aforos_albujon from %s", aforos_csv)


# ─── Source 1: HTML monitoring table ──────────────────────────────────────────

def scrape_html_table(conn: sqlite3.Connection) -> dict:
    """Scrape the most recent parameter values from the HTML monitoring page.

    The page uses CSS Flexbox/Grid layout — there is no <table> element.
    We walk the DOM looking for parameter name + value pairs.
    """
    url = "https://canalmarmenor.carm.es/monitorizacion/monitorizacion-de-parametros/"
    log.info("[html] Fetching %s", url)

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    today_year = date.today().year

    # Collect all text nodes in the page to find parameter/value pairs.
    # The page layout places parameter names and values in sibling divs
    # within a shared flex/grid container.
    # Strategy: look for containers that hold a known parameter name,
    # then extract all date+value pairs from the same container.

    inserted = 0

    # Find all elements whose text matches a known parameter name
    for param_es, col_name in PARAM_KEYWORDS.items():
        # Search for an element with this parameter keyword
        elements = soup.find_all(string=re.compile(param_es, re.IGNORECASE))
        for el in elements:
            parent = el.parent
            if parent is None:
                continue
            # Walk up to a container that also holds date and value siblings
            container = parent.find_parent(
                lambda tag: tag.name in ("div", "section")
                and len(tag.find_all(string=re.compile(r"\d{1,2}\s+\w{3}", re.I))) > 0
            )
            if container is None:
                continue

            # Extract date strings and numeric value strings from the container
            date_strings = container.find_all(
                string=re.compile(r"^\s*\d{1,2}\s+[a-záéíóúñ]{3}\s*$", re.IGNORECASE)
            )
            value_strings = container.find_all(
                string=re.compile(r"^\s*\d[\d,\.]*\s*$")
            )

            for d_str, v_str in zip(date_strings, value_strings):
                iso_date = spanish_date_to_iso(d_str.strip(), today_year)
                value    = parse_float(v_str.strip())
                if iso_date is None or value is None:
                    continue

                # Ensure a row exists for this (fecha, fuente) pair
                conn.execute(
                    "INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, 'html')",
                    (iso_date,),
                )
                conn.execute(
                    f"UPDATE parametros_laguna SET {col_name} = ? WHERE fecha = ? AND fuente = 'html' AND {col_name} IS NULL",
                    (value, iso_date),
                )
                inserted += 1
            break  # stop after first matching container per parameter

    conn.commit()
    log.info("[html] Inserted/updated %d parameter values", inserted)
    return {"source": "html", "new_records": inserted, "error": None}


# ─── Source 2: CdG weekly PDFs ────────────────────────────────────────────────

def scrape_cdg_pdf(conn: sqlite3.Connection) -> dict:
    """Download and parse the most recent CdG weekly PDF report.

    Tries the last 10 days in reverse order. Stops at the first
    PDF that exists and can be parsed.
    """
    base_url = "https://canalmarmenor.carm.es/wp-content/uploads/Informe_CdG_{DD}_{MM}_{YYYY}.pdf"
    today = date.today()

    pdf_path = None
    report_date = None

    for delta in range(10):
        candidate = today - timedelta(days=delta)
        url = base_url.format(
            DD=candidate.strftime("%d"),
            MM=candidate.strftime("%m"),
            YYYY=candidate.strftime("%Y"),
        )
        log.info("[cdg] Checking %s", url)
        try:
            head = requests.head(url, timeout=10)
            if head.status_code == 200:
                log.info("[cdg] Found PDF: %s", url)
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                pdf_path = PDF_DIR / f"Informe_CdG_{candidate.isoformat()}.pdf"
                pdf_path.write_bytes(resp.content)
                report_date = candidate.isoformat()
                break
        except requests.RequestException as e:
            log.warning("[cdg] Request error for %s: %s", url, e)

    if pdf_path is None:
        raise RuntimeError("No CdG PDF found in the last 10 days")

    # Check if we already have this date in the DB
    existing = conn.execute(
        "SELECT id FROM parametros_laguna WHERE fecha = ? AND fuente = 'cdg'",
        (report_date,),
    ).fetchone()
    if existing:
        log.info("[cdg] Date %s already in DB, skipping", report_date)
        return {"source": "cdg", "new_records": 0, "error": None}

    inserted = _parse_laguna_pdf(conn, pdf_path, report_date, "cdg")
    log.info("[cdg] Inserted %d records for %s", inserted, report_date)
    return {"source": "cdg", "new_records": inserted, "error": None}


# ─── Source 3: IMIDA weekly PDFs ──────────────────────────────────────────────

def scrape_imida_pdfs(conn: sqlite3.Connection) -> dict:
    """Scrape the IMIDA report listing and download new PDFs.

    URL pattern: Informe_detalle_total_YYYYMMDD_imida.pdf
    Already-processed dates are skipped.
    """
    listing_url = "https://canalmarmenor.carm.es/ciencia/informes-monitorizacion-imida/"
    log.info("[imida] Fetching listing: %s", listing_url)

    resp = requests.get(listing_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Find all PDF links matching the IMIDA pattern
    pattern = re.compile(r"Informe_detalle_total_(\d{8})_imida\.pdf", re.IGNORECASE)
    links = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            raw_date = m.group(1)  # YYYYMMDD
            iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            links.append((iso_date, a["href"]))

    log.info("[imida] Found %d PDF links", len(links))

    # Dates already in the database
    existing_dates = {
        row["fecha"]
        for row in conn.execute(
            "SELECT fecha FROM parametros_laguna WHERE fuente = 'imida'"
        ).fetchall()
    }

    total_inserted = 0
    for iso_date, href in sorted(links):
        if iso_date in existing_dates:
            continue

        # Build absolute URL if needed
        url = href if href.startswith("http") else "https://canalmarmenor.carm.es" + href
        log.info("[imida] Downloading %s", url)

        pdf_path = PDF_DIR / f"Informe_imida_{iso_date}.pdf"
        if not pdf_path.exists():
            try:
                r = requests.get(url, timeout=120)
                r.raise_for_status()
                pdf_path.write_bytes(r.content)
            except requests.RequestException as e:
                log.warning("[imida] Failed to download %s: %s", url, e)
                continue

        inserted = _parse_imida_pdf(conn, pdf_path, iso_date)
        total_inserted += inserted
        log.info("[imida] Inserted %d records for %s", inserted, iso_date)

    log.info("[imida] Total new records: %d", total_inserted)
    return {"source": "imida", "new_records": total_inserted, "error": None}


# ─── Source 4: Albujón flow PDFs ──────────────────────────────────────────────

def scrape_aforos_pdfs(conn: sqlite3.Connection) -> dict:
    """Scrape and parse Rambla del Albujón flow/nutrient PDFs.

    The aforos page lists PDFs with pattern DD_MM_YYYY.pdf (some have -1 suffix).
    """
    listing_url = "https://canalmarmenor.carm.es/monitorizacion/monitorizacion-de-parametros/aforos/"
    log.info("[aforos] Fetching listing: %s", listing_url)

    resp = requests.get(listing_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Match filenames like 12_03_2026.pdf or 29_01_2025-1.pdf
    pattern = re.compile(r"(\d{2})_(\d{2})_(\d{4})(?:-\d+)?\.pdf", re.IGNORECASE)
    links = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            iso_date = f"{year}-{month}-{day}"
            links.append((iso_date, a["href"]))

    log.info("[aforos] Found %d PDF links", len(links))

    existing_dates = {
        row["fecha"]
        for row in conn.execute("SELECT fecha FROM aforos_albujon").fetchall()
    }

    total_inserted = 0
    for iso_date, href in sorted(links):
        if iso_date in existing_dates:
            continue

        url = href if href.startswith("http") else "https://canalmarmenor.carm.es" + href
        log.info("[aforos] Downloading %s", url)

        pdf_path = PDF_DIR / f"aforos_{iso_date}.pdf"
        if not pdf_path.exists():
            try:
                r = requests.get(url, timeout=60)
                r.raise_for_status()
                pdf_path.write_bytes(r.content)
            except requests.RequestException as e:
                log.warning("[aforos] Failed to download %s: %s", url, e)
                continue

        inserted = _parse_aforos_pdf(conn, pdf_path, iso_date)
        total_inserted += inserted
        log.info("[aforos] Inserted %d records for %s", inserted, iso_date)

    log.info("[aforos] Total new records: %d", total_inserted)
    return {"source": "aforos", "new_records": total_inserted, "error": None}


# ─── PDF parsers ──────────────────────────────────────────────────────────────

def _parse_laguna_pdf(
    conn: sqlite3.Connection,
    pdf_path: Path,
    report_date: str,
    source: str,
) -> int:
    """Extract lagoon parameters from a CdG or IMIDA PDF.

    Iterates all pages, looks for tables containing known parameter
    keywords. Returns the number of rows inserted.
    """
    row_data: dict[str, Optional[float]] = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if not row:
                            continue
                        # The first cell usually contains the parameter name
                        first_cell = str(row[0] or "").lower()
                        for keyword, col_name in PARAM_KEYWORDS.items():
                            if keyword in first_cell:
                                # Try each remaining cell for a numeric value
                                for cell in row[1:]:
                                    val = parse_float(str(cell or ""))
                                    if val is not None:
                                        row_data[col_name] = val
                                        break
    except Exception as e:
        raise RuntimeError(f"pdfplumber failed on {pdf_path.name}: {e}") from e

    if not row_data:
        raise RuntimeError(
            f"PDF parsed but no known parameters found in {pdf_path.name}. "
            "The PDF format may have changed."
        )

    conn.execute(
        """INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, ?)""",
        (report_date, source),
    )
    for col, val in row_data.items():
        conn.execute(
            f"UPDATE parametros_laguna SET {col} = ? WHERE fecha = ? AND fuente = ? AND {col} IS NULL",
            (val, report_date, source),
        )
    conn.commit()
    return 1


def _parse_imida_pdf(conn: sqlite3.Connection, pdf_path: Path, report_date: str) -> int:
    """Extract lagoon parameters from an IMIDA weekly PDF.

    IMIDA PDFs have a 'VALORACIÓN PRELIMINAR' summary table where parameters
    are spread across columns (not rows). Each parameter spans 3 columns;
    the label appears at header_index but the value is at header_index - 1
    due to merged cells. We average the MÍNIMO and MÁXIMO rows.
    """
    # Maps substrings in the header row to DB column names
    IMIDA_COLS = {
        "temp":       "temperatura",
        "turbidez":   "turbidez_ftu",
        "oxígeno":    "oxigeno_mg_l",
        "oxigeno":    "oxigeno_mg_l",
        "clorofila":  "clorofila_ug_l",
        "salinidad":  "salinidad",
    }

    row_data = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    # Identify the VALORACIÓN PRELIMINAR table
                    first_cell = str(table[0][0] if table and table[0] else "").lower()
                    if "valoraci" not in first_cell and "preliminar" not in first_cell:
                        continue

                    # Find header row (contains "Temp." and "Clorofila")
                    header = None
                    min_row = None
                    max_row = None
                    for row in table:
                        cells = [str(c or "").strip() for c in row]
                        joined = " ".join(cells).lower()
                        if "temp" in joined and "clorofila" in joined:
                            header = cells
                        elif header and "mínimo" in joined:
                            min_row = cells
                        elif header and "máximo" in joined:
                            max_row = cells

                    if header is None or (min_row is None and max_row is None):
                        continue

                    # Build a map: col_name → column index in the header
                    param_col = {}
                    for i, cell in enumerate(header):
                        for keyword, col_name in IMIDA_COLS.items():
                            if keyword in cell.lower() and col_name not in param_col:
                                param_col[col_name] = i

                    # Values sit at header_index - 1 (merged cell offset)
                    for col_name, hi in param_col.items():
                        vi = hi - 1  # value index
                        if vi < 0:
                            continue
                        v_min = parse_float(min_row[vi]) if min_row and vi < len(min_row) else None
                        v_max = parse_float(max_row[vi]) if max_row and vi < len(max_row) else None
                        if v_min is not None and v_max is not None:
                            row_data[col_name] = (v_min + v_max) / 2
                        elif v_min is not None:
                            row_data[col_name] = v_min
                        elif v_max is not None:
                            row_data[col_name] = v_max

                    if row_data:
                        break
                if row_data:
                    break

    except Exception as e:
        raise RuntimeError(f"pdfplumber failed on {pdf_path.name}: {e}") from e

    if not row_data:
        raise RuntimeError(
            f"PDF parsed but 'VALORACIÓN PRELIMINAR' table not found in {pdf_path.name}. "
            "The PDF format may have changed."
        )

    conn.execute(
        "INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, 'imida')",
        (report_date,),
    )
    for col, val in row_data.items():
        conn.execute(
            f"UPDATE parametros_laguna SET {col} = ? WHERE fecha = ? AND fuente = 'imida' AND {col} IS NULL",
            (val, report_date),
        )
    conn.commit()
    return 1


def _parse_aforos_pdf(conn: sqlite3.Connection, pdf_path: Path, report_date: str) -> int:
    """Extract nitrate values from an aforos lab analysis PDF.

    These PDFs are IMIDA lab reports, one page per sampling station (A01, A04…).
    Each page embeds multiple parameters in a single text-blob cell separated by
    newlines. Nitrates are reported in µmol NO3/L; we convert to mg/L and average
    across all stations. Caudal (flow) is not available in these PDFs.

    Conversion: 1 µmol NO3/L × 62.004 g/mol / 1000 = 0.062004 mg/L
    """
    # Match the numeric value that appears right before "µmol NO3".
    # Handles both formats:
    #   old (2024): "Nitratos ... 11.6 ± 12% µmol NO3/L"
    #   new (2025): "Nitratos ... 18.554 µmol NO3/L"
    #   with limit:  "Nitratos ... < 0.403 µmol NO3/L"
    NITRATE_RE = re.compile(
        r"(<?\s*[\d]+(?:[.,]\d+)?)\s+(?:±\s+[\d.]+%\s+)?µmol\s*NO3",
        re.IGNORECASE,
    )
    UMOL_TO_MGL = 0.062004  # µmol NO3/L → mg/L

    nitrate_values = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extract all text from the page (handles text-blob cells)
                text = page.extract_text() or ""
                for m in NITRATE_RE.finditer(text):
                    raw = m.group(1).replace("<", "").strip()
                    val = parse_float(raw)
                    if val is not None:
                        nitrate_values.append(val * UMOL_TO_MGL)
    except Exception as e:
        raise RuntimeError(f"pdfplumber failed on {pdf_path.name}: {e}") from e

    if not nitrate_values:
        raise RuntimeError(
            f"PDF parsed but no nitrate values found in {pdf_path.name}. "
            "The PDF format may have changed."
        )

    avg_nitratos = sum(nitrate_values) / len(nitrate_values)
    log.info("[aforos] %s: %d stations, avg nitratos = %.4f mg/L",
             pdf_path.name, len(nitrate_values), avg_nitratos)

    conn.execute(
        "INSERT OR IGNORE INTO aforos_albujon (fecha, caudal_l_s, nitratos_mg_l) VALUES (?, NULL, ?)",
        (report_date, avg_nitratos),
    )
    conn.commit()
    return 1


# ─── CSV export ───────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection):
    """Write parametros.csv and aforos.csv to the datos/ directory.

    These static files are committed to the repo and read by index.html
    via PapaParse — no backend needed.
    """
    params_path = DATA_DIR / "parametros.csv"
    aforos_path = DATA_DIR / "aforos.csv"

    # Export parametros_laguna
    rows = conn.execute(
        """SELECT fecha, fuente, temperatura, salinidad, clorofila_ug_l,
                  oxigeno_mg_l, turbidez_ftu, transparencia_m
           FROM parametros_laguna
           ORDER BY fecha ASC"""
    ).fetchall()
    with open(params_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "fuente", "temperatura", "salinidad",
                         "clorofila_ug_l", "oxigeno_mg_l", "turbidez_ftu", "transparencia_m"])
        for row in rows:
            writer.writerow([row[k] if row[k] is not None else "" for k in row.keys()])
    log.info("Exported %d rows to %s", len(rows), params_path)

    # Export aforos_albujon
    rows = conn.execute(
        "SELECT fecha, caudal_l_s, nitratos_mg_l FROM aforos_albujon ORDER BY fecha ASC"
    ).fetchall()
    with open(aforos_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "caudal_l_s", "nitratos_mg_l"])
        for row in rows:
            writer.writerow([row[k] if row[k] is not None else "" for k in row.keys()])
    log.info("Exported %d rows to %s", len(rows), aforos_path)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Mar Menor scraper starting ===")
    conn = get_db()

    # Seed DB from existing CSVs so we don't reprocess old data
    seed_from_csv(conn)

    # Run each scraper independently; failures do not abort the others
    scrapers = [
        ("html",   scrape_html_table),
        ("cdg",    scrape_cdg_pdf),
        ("imida",  scrape_imida_pdfs),
        ("aforos", scrape_aforos_pdfs),
    ]

    results = []
    for source_name, fn in scrapers:
        try:
            result = fn(conn)
            log_result(conn, source_name, result["new_records"], None)
            results.append(result)
        except Exception as e:
            error_msg = str(e)
            log.error("[%s] FAILED: %s", source_name, error_msg)
            log_result(conn, source_name, 0, error_msg)
            results.append({"source": source_name, "new_records": 0, "error": error_msg})

    # Export updated CSVs
    export_csv(conn)
    conn.close()

    # Print summary
    print("\n" + "=" * 50)
    print("DOWNLOAD SUMMARY")
    print("=" * 50)
    for r in results:
        status = "OK" if r["error"] is None else "FAILED"
        print(f"  [{status}] {r['source']}: {r['new_records']} new records", end="")
        if r["error"]:
            print(f" — {r['error']}")
        else:
            print()
    print("=" * 50)


if __name__ == "__main__":
    main()
