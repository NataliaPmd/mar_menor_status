"""Source: UPCT historical CSV scraper (2016–2024)."""

import logging
import sqlite3
from datetime import datetime

import requests

from .config import UPCT_BASE, UPCT_VARS
from .utils import _safe_col, parse_float

log = logging.getLogger(__name__)


def scrape_upct_csvs(conn: sqlite3.Connection) -> dict:
    """Download UPCT historical parameter CSVs (2016–2024).

    Each variable is a separate CSV with columns: Fecha, Medias, Desviaciones.
    Dates use YYYY/MM/DD format. Transparency values are negative depths;
    we store abs(value). Existing records are not overwritten (INSERT OR IGNORE).
    """
    total_inserted = 0

    for var_name, (col_name, negate) in UPCT_VARS.items():
        url = UPCT_BASE.format(var=var_name)
        log.info("[upct] Downloading %s", url)

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.warning("[upct] Failed to download %s: %s", url, e)
            continue

        inserted = 0
        for line in resp.text.splitlines()[1:]:   # skip header
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 2:
                continue
            raw_date, raw_val = parts[0].strip(), parts[1].strip()

            try:
                d = datetime.strptime(raw_date, "%Y/%m/%d")
                iso_date = d.strftime("%Y-%m-%d")
            except ValueError:
                log.debug("[upct] Unparseable date: %r", raw_date)
                continue

            val = parse_float(raw_val)
            if val is None:
                continue
            if negate:
                val = abs(val)   # negative depth → positive metres

            conn.execute(
                "INSERT OR IGNORE INTO parametros_laguna (fecha, fuente) VALUES (?, 'upct')",
                (iso_date,),
            )
            safe = _safe_col(col_name)
            rows = conn.execute(
                f"UPDATE parametros_laguna SET {safe} = ? "
                f"WHERE fecha = ? AND fuente = 'upct' AND {safe} IS NULL",
                (val, iso_date),
            ).rowcount
            inserted += rows

        conn.commit()
        log.info("[upct] %s: %d new values", var_name, inserted)
        total_inserted += inserted

    return {"source": "upct", "new_records": total_inserted, "error": None}
