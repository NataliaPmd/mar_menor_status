"""Source 4 (part 1): Albujón flow PDF scraper."""

import logging
import re
import sqlite3

import requests
from bs4 import BeautifulSoup

from .config import PDF_DIR
from .pdf_parsers import parse_aforos_pdf
from .utils import _download_pdf

log = logging.getLogger(__name__)


def scrape_aforos_pdfs(conn: sqlite3.Connection) -> dict:
    """Scrape and parse Rambla del Albujón nitrate lab PDFs.

    The aforos page lists PDFs with pattern DD_MM_YYYY.pdf (some have -1 suffix).
    """
    listing_url = "https://canalmarmenor.carm.es/monitorizacion/monitorizacion-de-parametros/aforos/"
    log.info("[aforos] Fetching listing: %s", listing_url)

    resp = requests.get(listing_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    pattern = re.compile(r"(\d{2})_(\d{2})_(\d{4})(?:-\d+)?\.pdf", re.IGNORECASE)
    links = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            iso_date = f"{year}-{month}-{day}"
            links.append((iso_date, a["href"]))

    log.info("[aforos] Found %d PDF links", len(links))

    # Skip dates that already have both nitrates and phosphates populated
    complete_dates = {
        row["fecha"]
        for row in conn.execute(
            "SELECT fecha FROM aforos_albujon WHERE nitratos_mg_l IS NOT NULL AND fosfatos_mg_l IS NOT NULL"
        ).fetchall()
    }

    total_inserted = 0
    for iso_date, href in sorted(links):
        if iso_date in complete_dates:
            continue

        url = href if href.startswith("http") else "https://canalmarmenor.carm.es" + href
        log.info("[aforos] Downloading %s", url)

        pdf_path = PDF_DIR / f"aforos_{iso_date}.pdf"
        if not _download_pdf(url, pdf_path, "aforos"):
            continue

        inserted = parse_aforos_pdf(conn, pdf_path, iso_date)
        total_inserted += inserted
        log.info("[aforos] Inserted %d records for %s", inserted, iso_date)

    log.info("[aforos] Total new records: %d", total_inserted)
    return {"source": "aforos", "new_records": total_inserted, "error": None}
