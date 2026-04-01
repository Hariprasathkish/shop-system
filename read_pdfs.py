import pdfplumber
import os

pdf_files = ["static/bills/bill_18_2026-03-16_2026-04-15.pdf", "static/bills/bill_21_2026-02-01_2026-02-28.pdf"]

for pdf_path in pdf_files:
    print(f"\n--- Reading {pdf_path} ---")
    if os.path.exists(pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            text = page.extract_text()
            print(text)
    else:
        print("File not found.")
