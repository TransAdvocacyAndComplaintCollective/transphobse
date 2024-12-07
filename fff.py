import os
import re
import csv

from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF using PyPDF2."""
    extracted_text = []
    try:
        pdf_reader = PdfReader(pdf_path)
        for page in pdf_reader.pages:
            page_text = page.extract_text()
            if page_text:
                extracted_text.append(page_text)
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
    return "\n".join(extracted_text)


def parse_programme_segments(text, pdf_path):
    """Parse text into segments based on programme patterns."""
    # Define regex patterns
    date_pattern = r"(((|\d{1,2}|(\d{1,2}[A-z ]*)\s(and|-|–|&)\s)?(\d{1,2} [A-z ]* \d{4}?|\d{1,2}? [A-z ]* \d{4})[a-z ]*)|Date withheld)"
    station_pattern = r"[A-Za-z0-9&'/)(.’\- ]+"
    programme_pattern = r"[A-Za-z0-9?&,:/“’”()\-'’ ]+"

    patterns = {
        "prog_station_date": re.compile(
            rf"^(?P<programme>{programme_pattern}),\s*(?P<station>{station_pattern}),\s*(?P<date>{date_pattern})$"
        ),
        "prog_date": re.compile(
            rf"^(?P<programme>{programme_pattern}),\s*(?P<date>{date_pattern})$"
        ),
        "station_prog_date": re.compile(
            rf"^(?P<station>{station_pattern}),\s*(?P<programme>{programme_pattern}),\s*(?P<date>{date_pattern})$"
        ),
        "prog_wip": re.compile(
            rf"^(?P<programme>{programme_pattern})\s+\(Working title\)$"
        ),
        "red_button": re.compile(
            rf"^(?P<station>BBC Red Button(?: text service closure)?)$"
        ),
        "site": re.compile(
            rf"^(?P<programme>{programme_pattern})\s+,bbc\.co\.uk$"
        ),
    }

    lines = text.split("\n")
    segments = []
    current_segment = None

    for line in lines:
        line = line.strip()
        if not line:
            continue  # Skip empty lines

        match = None
        for pattern_name, pattern in patterns.items():
            match = pattern.match(line)
            if match:
                break

        if match:
            # Save the current segment before starting a new one
            if current_segment:
                segments.append(current_segment)

            # Start a new segment
            current_segment = {
                "Programme": match.group("programme").strip() if "programme" in match.groupdict() else None,
                "Station": match.group("station").strip() if "station" in match.groupdict() else None,
                "Date": match.group("date").strip() if "date" in match.groupdict() else None,
                "Text Between": "",
                "PDF Path": pdf_path,
            }
        else:
            # Accumulate text for the current segment
            if current_segment:
                current_segment["Text Between"] += line + "\n"

    # Append the last segment if it exists
    if current_segment:
        segments.append(current_segment)

    return segments


def process_pdfs(pdf_folder, output_csv):
    """Process all PDFs in a folder and save results to a CSV file."""
    all_segments = []
    for file_name in os.listdir(pdf_folder):
        if file_name.endswith(".pdf") and ("ecu" in file_name):
            pdf_path = os.path.join(pdf_folder, file_name)
            print(f"Processing: {pdf_path}")
            text = extract_text_from_pdf(pdf_path)
            if text:
                segments = parse_programme_segments(text, pdf_path)
                all_segments.extend(segments)

    # Save to CSV
    if all_segments:
        with open(output_csv, mode="w", newline="", encoding="utf-8") as csv_file:
            fieldnames = ["PDF Path", "Programme", "Station", "Date", "Text Between"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_segments)
        print(f"Data successfully saved to {output_csv}")
    else:
        print("No data extracted from the PDFs.")

ddd = extract_text_from_pdf ("/home/lucy/Desktop/transphobse/data/bbc_complaint_pdfs/7 - 20 June 2021.pdf")
print(ddd)