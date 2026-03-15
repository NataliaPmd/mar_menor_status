"""
Mar Menor ecological monitoring scraper.

Downloads data from official sources, stores in SQLite,
and exports static CSVs for the public dashboard.

Run with: python scraper.py
"""

import logging

from scrapers.db import export_csv, get_db, log_result, seed_from_csv
from scrapers.source_aemet import scrape_aemet_precipitation
from scrapers.source_aforos import scrape_aforos_pdfs
from scrapers.source_cdg import scrape_cdg_pdf
from scrapers.source_html import scrape_html_table
from scrapers.source_imida import scrape_imida_pdfs
from scrapers.source_upct import scrape_upct_csvs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    log.info("=== Mar Menor scraper starting ===")
    conn = get_db()

    # Seed DB from existing CSVs so we don't reprocess old data
    seed_from_csv(conn)

    # Run each scraper independently; failures do not abort the others
    scrapers = [
        ("upct",   scrape_upct_csvs),           # historical 2016–2024 (run first so newer sources can overwrite)
        ("aemet",  scrape_aemet_precipitation),  # daily precipitation from AEMET
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
