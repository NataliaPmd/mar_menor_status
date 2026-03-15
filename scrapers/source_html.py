"""Source 1: HTML monitoring table scraper."""

import logging
import re
import sqlite3
from datetime import date

import requests
from bs4 import BeautifulSoup

from .utils import _safe_col, parse_float, spanish_date_to_iso

log = logging.getLogger(__name__)


def scrape_html_table(conn: sqlite3.Connection) -> dict:
    """Scrape the most recent parameter values from the HTML monitoring page.

    The page shows a comparison table: same 4 dates for year1 (e.g. 2026)
    and year2 (e.g. 2025). For each parameter the text layout is:

        [param name]
        [date1] [date2] [date3] [date4]   <- 4 dates, year1
        [val1]  [val2]  [val3]  [val4]    <- 4 values, year1 ('-' = missing)
        [date1] [date2] [date3] [date4]   <- same dates, year2
        [val1]  [val2]  [val3]  [val4]    <- 4 values, year2

    We parse the raw page text line by line to extract this structure.
    Stale html records are deleted before inserting fresh ones.
    """
    url = "https://canalmarmenor.carm.es/monitorizacion/monitorizacion-de-parametros/"
    log.info("[html] Fetching %s", url)

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    lines = [l.strip() for l in soup.get_text(separator="\n").splitlines() if l.strip()]

    PARAM_MAP = {
        "transparencia": "transparencia_m",
        "turbidez":      "turbidez_ftu",
        "clorofila":     "clorofila_ug_l",
        "temperatura":   "temperatura",
        "salinidad":     "salinidad",
        "oxígeno":       "oxigeno_mg_l",
        "oxigeno":       "oxigeno_mg_l",
    }
    DATE_RE  = re.compile(r"^\d{1,2}\s+[a-záéíóú]{3}$", re.IGNORECASE)
    VALUE_RE = re.compile(r"^-$|^\d+[,.]?\d*$")
    YEAR_RE  = re.compile(r"^20\d{2}$")

    start_idx = next(
        (i for i, l in enumerate(lines) if "últimos datos" in l.lower()), None
    )
    if start_idx is None:
        raise RuntimeError("Could not find 'Últimos datos' section in the HTML page")

    year1 = year2 = None
    for line in lines[start_idx: start_idx + 15]:
        if YEAR_RE.match(line):
            if year1 is None:
                year1 = int(line)
            elif year2 is None:
                year2 = int(line)
                break
    year1 = year1 or date.today().year
    year2 = year2 or (year1 - 1)

    # Collect all (col, iso_date, value) tuples during parsing.
    # The DELETE + bulk insert is done atomically afterwards so that a
    # parse failure mid-way cannot leave the DB with missing html rows.
    pending: list[tuple[str, str, float]] = []

    def _collect(col, dates, values, year) -> int:
        safe = _safe_col(col)
        count = 0
        for d_str, v_str in zip(dates, values):
            iso = spanish_date_to_iso(d_str, year)
            val = parse_float(v_str)
            if iso is None or val is None:
                continue
            pending.append((safe, iso, val))
            count += 1
        return count

    inserted   = 0
    col_name   = None
    dates1: list = []
    values1: list = []
    dates2: list = []
    values2: list = []
    zone = 0  # 1 = collecting year1, 2 = collecting year2

    for line in lines[start_idx:]:
        matched_col = None
        line_lower = line.lower()
        if "(" in line or "ºc" in line_lower:
            for keyword, col in PARAM_MAP.items():
                if keyword in line_lower:
                    matched_col = col
                    break

        if matched_col:
            if col_name:
                inserted += _collect(col_name, dates1, values1, year1)
                inserted += _collect(col_name, dates2, values2, year2)
            col_name = matched_col
            dates1, values1, dates2, values2 = [], [], [], []
            zone = 1
            continue

        if col_name is None:
            continue

        if line == "Todos los datos":
            break

        if DATE_RE.match(line):
            if zone == 1 and len(dates1) < 4:
                dates1.append(line)
            elif zone == 2 and len(dates2) < 4:
                dates2.append(line)

        elif VALUE_RE.match(line):
            if zone == 1 and len(values1) < 4:
                values1.append(line)
                if len(values1) == 4:
                    zone = 2
            elif zone == 2 and len(values2) < 4:
                values2.append(line)

    if col_name:
        inserted += _collect(col_name, dates1, values1, year1)
        inserted += _collect(col_name, dates2, values2, year2)

    with conn:
        conn.execute("DELETE FROM parametros_laguna WHERE fuente = 'html'")
        for safe, iso, val in pending:
            conn.execute(
                "INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, 'html')",
                (iso,),
            )
            conn.execute(
                f"UPDATE parametros_laguna SET {safe} = ? "
                f"WHERE fecha = ? AND fuente = 'html' AND {safe} IS NULL",
                (val, iso),
            )

    log.info("[html] Inserted %d parameter values (year1=%d, year2=%d)", inserted, year1, year2)
    return {"source": "html", "new_records": inserted, "error": None}
