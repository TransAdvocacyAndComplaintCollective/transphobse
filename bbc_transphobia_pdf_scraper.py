import os
import re
import requests
import time
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote  # Added unquote to handle URL-decoded filenames
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
import camelot
from keywords import KEYWORDS
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBox, LTTextLine, LAParams

# Configuration
BASE_URL = "https://www.bbc.co.uk/contact/complaint-service-reports?page={}"

PDF_DIR = 'pdfs'
FILTERED_PDF_DIR = 'filtered_pdfs'
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(FILTERED_PDF_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to download PDFs
def download_pdf(pdf_url):
    local_filename = os.path.join(PDF_DIR, unquote(os.path.basename(pdf_url)))  # Use unquote to decode URL-encoded filenames
    try:
        response = requests.get(pdf_url, timeout=10)
        response.raise_for_status()
        with open(local_filename, 'wb') as f:
            f.write(response.content)
        logging.info(f"Downloaded: {local_filename}")
        return local_filename
    except requests.RequestException as e:
        logging.error(f"Failed to download {pdf_url}: {e}")
    return None

# Function to check if the PDF contains any keyword
def check_pdf_for_keywords(file_path, KEYWORDS):
    try:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None
        
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    for keyword in KEYWORDS:
                        if keyword.lower() in text.lower():
                            logging.info(f"Keyword '{keyword}' found in: {file_path}")
                            # Ensure sanitized file path for renaming
                            sanitized_filename = unquote(os.path.basename(file_path)).replace("%20", "_")
                            filtered_path = os.path.join(FILTERED_PDF_DIR, sanitized_filename)
                            if os.path.exists(file_path):  # Check if the file still exists
                                os.rename(file_path, filtered_path)
                            else:
                                logging.error(f"File not found when attempting to move: {file_path}")
                            return filtered_path
        # Remove the file if no keywords are found, but check if it exists first
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
    return None
import fitz


# Function to process each page of the website
def process_page(page_number):
    url = BASE_URL.format(page_number)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        pdf_links = [urljoin(url, link['href']) for link in soup.find_all('a', href=True) if link['href'].endswith('.pdf')]

        # Check if the only PDF link is 'Complaints_Framework_eng_0.pdf'
        if len(pdf_links) == 1 and 'Complaints_Framework_eng_0.pdf' in pdf_links[0]:
            logging.info("Only 'Complaints_Framework_eng_0.pdf' found on this page. Stopping the download.")
            return []

        return pdf_links
    except requests.RequestException as e:
        logging.error(f"Failed to process page {page_number}: {e}")
    return []


import fitz  # PyMuPDF

# Function to extract links, their associated text, and location from a page
def extract_links_with_text_and_location_from_page(page):
    links_with_text_and_location = []
    # Loop through each link annotation on the page
    for link in page.get_links():
        uri = link.get("uri", None)
        if uri:
            # Extract the rectangle area where the link is located
            rect = fitz.Rect(link["from"])
            # Use the rectangle to extract the associated text
            text = page.get_textbox(rect).strip()
            # Append the URI, its associated text, and location (rectangle coordinates) to the list
            links_with_text_and_location.append({
                "uri": uri,
                "text": text,
                "location": {"x0": rect.x0, "y0": rect.y0, "x1": rect.x1, "y1": rect.y1}
            })
    return links_with_text_and_location

# Function to extract non-link text from a page
def extract_non_link_text_from_page(page, link_rects):
    # Exclude text inside link rectangles to get non-link text
    non_link_text = ""
    text_instances = page.get_text("blocks")  # Get all text blocks from the page

    for inst in text_instances:
        rect = fitz.Rect(inst[:4])
        # Check if this block overlaps with any link rectangles
        if not any(rect.intersects(link_rect) for link_rect in link_rects):
            non_link_text += inst[4].strip() + "\n"
    
    return non_link_text.strip()

# Function to extract all links, their associated text, and non-linked text from the PDF
def extract_links_and_non_links_from_pdf(pdf_file):
    all_links_with_text_and_location = []
    all_non_link_text = []
    
    # Open the PDF file
    document = fitz.open(pdf_file)
    
    # Iterate through each page in the document
    for page_number in range(document.page_count):
        page = document[page_number]
        
        # Extract links, their associated text, and location from the current page
        page_links_with_text_and_location = extract_links_with_text_and_location_from_page(page)
        all_links_with_text_and_location.extend(page_links_with_text_and_location)
        
        # Extract non-link text from the current page
        link_rects = [fitz.Rect(link["location"]["x0"], link["location"]["y0"], link["location"]["x1"], link["location"]["y1"])
                      for link in page_links_with_text_and_location]
        non_link_text = extract_non_link_text_from_page(page, link_rects)
        all_non_link_text.append({"page": page_number + 1, "non_link_text": non_link_text})
    
    # Close the document
    document.close()

    # Print all extracted links, their associated text, and location
    print("Extracted Links with Text and Location:")
    for link_info in all_links_with_text_and_location:
        print(f"Text: {link_info['text']}, URL: {link_info['uri']}, Location: {link_info['location']}")
    
    # Print non-linked text
    print("\nExtracted Non-Linked Text:")
    for non_link in all_non_link_text:
        print(f"Page {non_link['page']}:")
        print(non_link['non_link_text'])
        print("-" * 80)

    return all_links_with_text_and_location, all_non_link_text  # Return the list of extracted links and non-link text



# Function to extract tables from PDFs and add URL columns
def extract_tables_from_pdf(pdf_path):
    extract_links_and_non_links_from_pdf(pdf_path)
    print(f"Extracting tables from {pdf_path}")
    extract_links_with_text_and_location_from_page(pdf_path)
    try:
        # Try both flavors for better accuracy
        tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')
        if not tables:
            tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
        print(dir(tables))

        if tables:
            for i, table in enumerate(tables):
                for col in table.df.columns:
                    table.df[col] = table.df[col].str.replace(r'\n', ' ', regex=True)
                df = table.df
                output_csv = f"{os.path.splitext(pdf_path)[0]}_table_{i}.csv"
                df.to_csv(output_csv, index=False)
                logging.info(f"Extracted and cleaned table {i} from {pdf_path} to {output_csv}")
        else:
            logging.warning(f"No tables found in {pdf_path} using both lattice and stream flavors.")
    except Exception as e:
        logging.error(f"Failed to extract tables from {pdf_path}: {e}")

# Main function
def main():
    page_number = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            pdf_links = process_page(page_number)
            if not pdf_links:
                logging.info(f"No more PDFs found. Stopping at page {page_number}.")
                break

            # Download and filter PDFs concurrently
            future_to_url = {executor.submit(download_pdf, pdf_url): pdf_url for pdf_url in pdf_links}
            for future in as_completed(future_to_url):
                pdf_path = future.result()
                if pdf_path:
                    filtered_path = check_pdf_for_keywords(pdf_path, KEYWORDS)
                    if filtered_path:
                        executor.submit(extract_tables_from_pdf, filtered_path)

            page_number += 1
            time.sleep(2)  # Be polite and don't overload the server

if __name__ == "__main__":
    main()
