# Quick diagnostic: prints all tables found in a PDF
# Usage: python3 debug_pdf.py pdfs/Informe_imida_2022-07-11.pdf

import sys
import pdfplumber

path = sys.argv[1] if len(sys.argv) > 1 else "pdfs/Informe_imida_2022-07-11.pdf"

with pdfplumber.open(path) as pdf:
    print(f"Pages: {len(pdf.pages)}")
    for i, page in enumerate(pdf.pages[:5]):  # first 5 pages only
        tables = page.extract_tables()
        if tables:
            print(f"\n--- Page {i+1}: {len(tables)} table(s) ---")
            for t, table in enumerate(tables):
                print(f"  Table {t+1}:")
                for row in table[:8]:  # first 8 rows per table
                    print("   ", row)
