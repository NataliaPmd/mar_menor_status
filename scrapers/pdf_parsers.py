"""PDF parsing helpers shared across sources."""

import logging
import re
import sqlite3
from pathlib import Path
from typing import Optional

import pdfplumber

from .config import PARAM_KEYWORDS
from .utils import _safe_col, parse_float

log = logging.getLogger(__name__)


def parse_laguna_pdf(
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
                        first_cell = str(row[0] or "").lower()
                        for keyword, col_name in PARAM_KEYWORDS.items():
                            if keyword in first_cell:
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
        "INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, ?)",
        (report_date, source),
    )
    for col, val in row_data.items():
        safe = _safe_col(col)
        conn.execute(
            f"UPDATE parametros_laguna SET {safe} = ? "
            f"WHERE fecha = ? AND fuente = ? AND {safe} IS NULL",
            (val, report_date, source),
        )
    conn.commit()
    return 1


def parse_imida_pdf(conn: sqlite3.Connection, pdf_path: Path, report_date: str) -> int:
    """Extract lagoon parameters from an IMIDA weekly PDF.

    IMIDA PDFs have a 'VALORACIÓN PRELIMINAR' summary table where parameters
    are spread across columns (not rows). Each parameter spans 3 columns;
    the label appears at header_index but the value is at header_index - 1
    due to merged cells. We average the MÍNIMO and MÁXIMO rows.
    """
    IMIDA_COLS = {
        "temp":          "temperatura",
        "turbidez":      "turbidez_ftu",
        "oxígeno":       "oxigeno_mg_l",
        "oxigeno":       "oxigeno_mg_l",
        "clorofila":     "clorofila_ug_l",
        "salinidad":     "salinidad",
        "transparencia": "transparencia_m",
    }

    row_data = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    first_cell = str(table[0][0] if table and table[0] else "").lower()
                    if "valoraci" not in first_cell and "preliminar" not in first_cell:
                        continue

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

                    param_col = {}
                    for i, cell in enumerate(header):
                        for keyword, col_name in IMIDA_COLS.items():
                            if keyword in cell.lower() and col_name not in param_col:
                                param_col[col_name] = i

                    # Values sit at header_index - 1 (merged cell offset)
                    for col_name, hi in param_col.items():
                        vi = hi - 1
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
        safe = _safe_col(col)
        conn.execute(
            f"UPDATE parametros_laguna SET {safe} = ? WHERE fecha = ? AND fuente = 'imida' AND {safe} IS NULL",
            (val, report_date),
        )
    conn.commit()
    return 1


def parse_aforos_pdf(conn: sqlite3.Connection, pdf_path: Path, report_date: str) -> int:
    """Extract nitrate values from an aforos lab analysis PDF.

    These PDFs are IMIDA lab reports, one page per sampling station (A01, A04…).
    Each page embeds multiple parameters in a single text-blob cell separated by
    newlines. Nitrates are reported in µmol NO3/L; we convert to mg/L and average
    across all stations. Caudal (flow) is not available in these PDFs.

    Conversion: 1 µmol NO3/L × 62.004 g/mol / 1000 = 0.062004 mg/L
    """
    NITRATE_RE = re.compile(
        r"(<?\s*[\d]+(?:[.,]\d+)?)\s+(?:±\s+[\d.]+%\s+)?µmol\s*NO3",
        re.IGNORECASE,
    )
    PHOSPHATE_RE = re.compile(
        r"(<?\s*[\d]+(?:[.,]\d+)?)\s+(?:±\s+[\d.]+%\s+)?µmol\s*PO4",
        re.IGNORECASE,
    )
    NO3_TO_MGL = 0.062004   # µmol NO3/L  → mg/L  (MW 62.004 g/mol)
    PO4_TO_MGL = 0.094971   # µmol PO4/L  → mg/L  (MW 94.971 g/mol)

    nitrate_values   = []
    phosphate_values = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for m in NITRATE_RE.finditer(text):
                    val = parse_float(m.group(1).replace("<", "").strip())
                    if val is not None:
                        nitrate_values.append(val * NO3_TO_MGL)
                for m in PHOSPHATE_RE.finditer(text):
                    val = parse_float(m.group(1).replace("<", "").strip())
                    if val is not None:
                        phosphate_values.append(val * PO4_TO_MGL)
    except Exception as e:
        raise RuntimeError(f"pdfplumber failed on {pdf_path.name}: {e}") from e

    if not nitrate_values:
        raise RuntimeError(
            f"PDF parsed but no nitrate values found in {pdf_path.name}. "
            "The PDF format may have changed."
        )

    avg_nitratos = sum(nitrate_values)   / len(nitrate_values)
    avg_fosfatos = sum(phosphate_values) / len(phosphate_values) if phosphate_values else None

    log.info("[aforos] %s: %d stations, avg nitratos = %.4f mg/L, avg fosfatos = %s mg/L",
             pdf_path.name, len(nitrate_values), avg_nitratos,
             f"{avg_fosfatos:.4f}" if avg_fosfatos is not None else "n/a")

    conn.execute(
        "INSERT OR IGNORE INTO aforos_albujon (fecha, caudal_l_s, nitratos_mg_l, fosfatos_mg_l) VALUES (?, NULL, ?, ?)",
        (report_date, avg_nitratos, avg_fosfatos),
    )
    if avg_fosfatos is not None:
        conn.execute(
            "UPDATE aforos_albujon SET fosfatos_mg_l = ? WHERE fecha = ? AND fosfatos_mg_l IS NULL",
            (avg_fosfatos, report_date),
        )
    conn.commit()
    return 1
