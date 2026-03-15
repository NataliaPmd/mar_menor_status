"""Source 2: CdG weekly PDF scraper."""

import logging
import sqlite3
from datetime import date, timedelta

import requests

from .config import PDF_DIR
from .pdf_parsers import parse_laguna_pdf

log = logging.getLogger(__name__)


def scrape_cdg_pdf(conn: sqlite3.Connection) -> dict:
    """Download and parse all missing CdG weekly PDF reports.

    Scans every day from the day after the last CdG date already in the DB
    (or 2025-04-29 as a safe backfill floor) up to today. Downloads any PDF
    that exists and hasn't been processed yet. This handles both backfill and
    ongoing weekly updates in one pass.
    """
    base_url = "https://canalmarmenor.carm.es/wp-content/uploads/Informe_CdG_{DD}_{MM}_{YYYY}.pdf"
    today = date.today()

    existing_dates = {
        row[0]
        for row in conn.execute(
            "SELECT fecha FROM parametros_laguna WHERE fuente = 'cdg'"
        ).fetchall()
    }

    scan_from = date(2025, 4, 29)
    log.info("[cdg] Scanning for PDFs from %s to %s", scan_from, today)

    total_inserted = 0
    candidate = scan_from
    while candidate <= today:
        iso = candidate.isoformat()
        if iso in existing_dates:
            candidate += timedelta(days=1)
            continue

        url = base_url.format(
            DD=candidate.strftime("%d"),
            MM=candidate.strftime("%m"),
            YYYY=candidate.strftime("%Y"),
        )
        try:
            head = requests.head(url, timeout=10)
            if head.status_code == 200:
                log.info("[cdg] Found PDF: %s", url)
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                pdf_path = PDF_DIR / f"Informe_CdG_{candidate.isoformat()}.pdf"
                pdf_path.write_bytes(resp.content)
                try:
                    inserted = parse_laguna_pdf(conn, pdf_path, iso, "cdg")
                    log.info("[cdg] Inserted %d records for %s", inserted, candidate)
                    total_inserted += inserted
                except RuntimeError as parse_err:
                    log.warning("[cdg] Skipping %s — parse failed: %s", iso, parse_err)
        except requests.RequestException as e:
            log.warning("[cdg] Request error for %s: %s", url, e)

        candidate += timedelta(days=1)

    if total_inserted == 0:
        log.info("[cdg] No new PDFs found")
    return {"source": "cdg", "new_records": total_inserted, "error": None}
