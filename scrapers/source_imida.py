"""Source 3: IMIDA weekly PDF scraper."""

import logging
import re
import sqlite3

import requests
from bs4 import BeautifulSoup

from .config import PDF_DIR
from .pdf_parsers import parse_imida_pdf
from .utils import _download_pdf

log = logging.getLogger(__name__)


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

    pattern = re.compile(r"Informe_detalle_total_(\d{8})_imida\.pdf", re.IGNORECASE)
    links = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            raw_date = m.group(1)  # YYYYMMDD
            iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            links.append((iso_date, a["href"]))

    log.info("[imida] Found %d PDF links", len(links))

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

        url = href if href.startswith("http") else "https://canalmarmenor.carm.es" + href
        log.info("[imida] Downloading %s", url)

        pdf_path = PDF_DIR / f"Informe_imida_{iso_date}.pdf"
        if not _download_pdf(url, pdf_path, "imida"):
            continue

        inserted = parse_imida_pdf(conn, pdf_path, iso_date)
        total_inserted += inserted
        log.info("[imida] Inserted %d records for %s", inserted, iso_date)

    log.info("[imida] Total new records: %d", total_inserted)
    return {"source": "imida", "new_records": total_inserted, "error": None}
