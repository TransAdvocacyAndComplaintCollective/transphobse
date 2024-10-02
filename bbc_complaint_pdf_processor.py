import os
import requests
import time
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
import camelot
import pandas as pd
import difflib

from utils.keywords import relative_keywords_score

# Configuration
BASE_URL = "https://www.bbc.co.uk/contact/complaint-service-reports?page={}"
PDF_DIR = 'data/bbc_complaint_pdfs'
CSV_OUTPUT_DIR = 'data/data/csv_outputs'
UNMATCHED_URLS_OUTPUT_DIR = 'data/unmatched_urls'
MASTER_CSV_OUTPUT = 'data/bbc_transphobia_complaint_service_reports_keywords_matched.csv'
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
os.makedirs(UNMATCHED_URLS_OUTPUT_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Expected headers
EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]

# Create the master CSV file
master_csv_headers = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome", "keywords", "anti-keywords", "Score", "type (HTML/PDF)", "url html", "url Pdf","URL","Response","Complaint"]
with open(MASTER_CSV_OUTPUT, 'w') as f:
    pd.DataFrame(columns=master_csv_headers).to_csv(f, index=False)

# Function to scrape the content from the web page pointed to by a URL
def scrape_web_content(url):
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Check if the request was successful
        soup = BeautifulSoup(response.content, "html.parser")
        return ' '.join([p.text for p in soup.find_all('p')])
    except requests.RequestException as e:
        logging.error(f"Failed to scrape {url}: {e}")
        return ""


# Function to download PDFs
def download_pdf(pdf_url, url_html, retries=3):
    local_filename = os.path.join(PDF_DIR, unquote(os.path.basename(pdf_url)))
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(pdf_url, timeout=10)
            response.raise_for_status()
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            return local_filename, url_html, pdf_url
        except requests.RequestException as e:
            attempt += 1
            logging.error(f"Failed to download {pdf_url}: {e}. Retrying {attempt}/{retries}...")
            time.sleep(2)
    return None, None, None

c = set()
# Function to try matching unmatched URLs
def match_unmatched_urls(cleaned_df, unmatched_links):
    """
    Try to match unmatched URLs to table rows by comparing the URL content or scraping the web page.

    Parameters:
    cleaned_df: DataFrame containing the cleaned table data
    unmatched_links: List of unmatched URLs to be processed

    Returns:
    DataFrame with matched URLs assigned to rows, and any remaining unmatched URLs
    """
    unmatched_links_temp = unmatched_links.copy()
    for url in unmatched_links:
        if url in c:
            continue
        c.add(url)
        print(f"Processing unmatched URL: {url}")
        if True: #"https://www.bbc.co.uk/contact/ecu/" in url:
            best_match_row = None
            best_match_score = 0
            url_text = url.lower()

            # First attempt: Fuzzy match against "Programme", "Service", "Date of Transmission" in the URL
            for index, row in cleaned_df.iterrows():
                row_text = ' '.join(map(str, [row.get("Programme", ""), row.get("Service", ""), row.get("Date of Transmission", "")])).lower()
                match_score = difflib.SequenceMatcher(None, url_text, row_text).ratio()

                if match_score > best_match_score and match_score > 0.5:  # Threshold of 0.5
                    best_match_row = index
                    best_match_score = match_score

            if best_match_row is None:
                # Second attempt: Scrape the web page and match its content
                web_content = scrape_web_content(url)
                for index, row in cleaned_df.iterrows():
                    row_text = ' '.join(map(str, row)).lower()
                    match_score = difflib.SequenceMatcher(None, web_content, row_text).ratio()

                    if match_score > best_match_score and match_score > 0.5:
                        best_match_row = index
                        best_match_score = match_score

            if best_match_row is None:
                # Third attempt: Best effort fuzzy match
                for index, row in cleaned_df.iterrows():
                    row_text = ' '.join(map(str, row)).lower()
                    match_score = difflib.SequenceMatcher(None, url_text, row_text).ratio()

                    if match_score > best_match_score:
                        best_match_row = index
                        best_match_score = match_score

            # Assign the URL to the best matching row if found
            if best_match_row is not None and pd.isna(cleaned_df.at[best_match_row, 'URL']):
                cleaned_df.at[best_match_row, 'URL'] = url
                print(f"Matched unmatched URL {url} to row {best_match_row} with score {best_match_score}")
                unmatched_links_temp.remove(url)

    # Capture unmatched URLs that couldn't be assigned
    remaining_unmatched_links = cleaned_df[cleaned_df['URL'].isna()]

    return cleaned_df, unmatched_links_temp

# Function to process each page of the website and extract PDF links
def process_page(page_number):
    url = BASE_URL.format(page_number)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        pdf_links = [urljoin(url, link['href']) for link in soup.find_all('a', href=True) if link['href'].endswith('.pdf')]
        pdf_links = [link for link in pdf_links if 'Complaints_Framework_eng' not in link]

        if not pdf_links:
            return [], url  # Return empty list and the page URL when no PDF links are found

        return pdf_links, url  # Return both the PDF links and the current page URL
    except requests.RequestException as e:
        logging.error(f"Failed to process page {page_number}: {e}")
    return [], None  # Return empty list and None when the request fails


# Function to clean newline characters from the data
def clean_newlines(df):
    return df.map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

# Function to check if two bounding boxes overlap
def is_bbox_overlap(bbox1, bbox2):
    x1_1, y1_1, x2_1, y2_1 = bbox1
    x1_2, y1_2, x2_2, y2_2 = bbox2
    return not (x2_1 < x1_2 or x2_2 < x1_1 or y2_1 < y1_2 or y2_2 < y1_1)

# Function to clean and assign headers to a table
def clean_and_assign_headers(df):
    df = clean_newlines(df)
    if len(df.columns) != len(EXPECTED_HEADERS):
        logging.warning(f"Table has {len(df.columns)} columns, but expected {len(EXPECTED_HEADERS)}")
        df.columns = ['Column' + str(i) for i in range(len(df.columns))]  # Auto-assign column names
    else:
        df.columns = EXPECTED_HEADERS
    return df

# Function to match URL to row based on bounding boxes
# Modified function to match URL to row based on bounding boxes
def match_url_to_row(table, pdf_links):
    page_links = pdf_links.get(table.page - 1, [])
    matched_links = []
    unmatched_links = set()
    output = []

    for row_id, rows in enumerate(table.cells):
        for cell in rows:
            cell_bbox = (cell.x1, cell.y1, cell.x2, cell.y2)
            matched = False
            for link in page_links:
                if link and isinstance(link, dict) and 'rect' in link and 'uri' in link:
                    link_bbox = (link['rect'][0], link['rect'][1], link['rect'][2], link['rect'][3])
                    if is_bbox_overlap(cell_bbox, link_bbox):
                        matched_links.append((cell_bbox, link['uri']))
                        print(f"Matched link {link['uri']} with cell bounding box {cell_bbox}")
                        output.append({"row_id": row_id, "cell_bbox": cell_bbox, "link": link['uri']})
                        matched = True
                        break
            if not matched and 'link' in locals() and 'uri' in link:
                unmatched_links.add(link['uri'] if link and 'uri' in link else None)

    return matched_links, list(unmatched_links), output

# Function to extract tables from a PDF and save them as CSVs, and ensure all pages are logged
def extract_tables_from_pdf(pdf_path, url_html, score, keywords, anti_keywords, pdf_url):
    try:
        # Initialize PDF reader
        reader = PdfReader(pdf_path)
        
        # Attempt to read tables using 'lattice' flavor first
        tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')

        # If no tables are found using 'lattice', try 'stream' flavor
        if not tables or all(len(table.df) == 0 for table in tables):
            tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')

        if not tables or all(len(table.df) == 0 for table in tables):
            logging.info(f"No valid tables found in {pdf_path}. Skipping file.")
            # Even if no tables are found, append the PDF-level data to the CSV
            append_to_master_csv({}, keywords, anti_keywords, score, 'full_pdf', url_html, pdf_path, pdf_url)
            return

        # Extract links from PDF annotations (all page links, even if they don't match table rows)
        output_pdf_links = extract_links_from_pdf(reader)

        # Process each detected table
        for i, table in enumerate(tables):
            if not table.df.empty:  # Only process tables with data
                # Get matched links and unmatched links for each table row
                matched_links, unmatched_links, matched_links_row_info = match_url_to_row(table, output_pdf_links)
                cleaned_df = clean_and_assign_headers(table.df)

                # Log all table rows, even if they don't have matching URLs
                cleaned_df['URL'] = None
                for link_info in matched_links_row_info:
                    row_id = link_info['row_id']
                    url = link_info['link']
                    if row_id < len(cleaned_df):
                        cleaned_df.at[row_id, 'URL'] = url

                # Log unmatched URLs and associate them with the PDF-level data
                cleaned_df, remaining_unmatched_links = match_unmatched_urls(cleaned_df, unmatched_links)
                
                # Log each remaining unmatched link as part of the overall PDF data
                for url in remaining_unmatched_links:
                    unmatched_url_file = os.path.join(UNMATCHED_URLS_OUTPUT_DIR, f"{os.path.basename(pdf_path)}_unmatched_urls.csv")
                    with open(unmatched_url_file, 'a') as f:
                        f.write(f"{url}\n")
                    
                    # Fetch the page content from the unmatched URLs
                    req = requests.get(url)
                    if req.status_code == 200:
                        final_score, keywords, anti_keywords = relative_keywords_score(req.text)
                        if final_score > 0:
                            append_to_master_csv({}, keywords, anti_keywords, final_score, 'unmatched_url', url, pdf_path, pdf_url)

                # Ensure all table rows are logged
                for index, row in cleaned_df.iterrows():
                    full_text = ' '.join(map(str, row.values))
                    final_score, keywords, anti_keywords = relative_keywords_score(full_text)
                    
                    # If the row contains a URL, verify and log it, regardless of match
                    if row.get("URL"):
                        url = row.get("URL")
                        req = requests.get(url)
                        text = req.text
                        html = BeautifulSoup(text, "html.parser")
                        # Extract relevant data, such as time and complaint response
                        time_data = html.find("time")
                        complaint_response = html.find("div", class_="complaint-response")
                        ecu_complaint = html.find("div", class_="ecu-complaint")
                        if complaint_response:
                            row["Response"] = complaint_response.text
                        if ecu_complaint:
                            row["Complaint"] = ecu_complaint.text
                        append_to_master_csv(row, keywords, anti_keywords, final_score, 'PDF_row', url_html, pdf_path, pdf_url)
                    else:
                        # Log rows even if they don't have URLs
                        append_to_master_csv(row, keywords, anti_keywords, final_score, 'PDF_row_no_url', url_html, pdf_path, pdf_url)

                # Save the cleaned table to a CSV for reference
                csv_file_path = os.path.join(CSV_OUTPUT_DIR, f"{os.path.basename(pdf_path)}_table_{i+1}.csv")
                cleaned_df.to_csv(csv_file_path, index=False)
                print(f"Saved and cleaned table {i+1} with URLs to {csv_file_path}")

    except Exception as e:
        logging.error(f"Failed to extract tables from {pdf_path}: {e}")
        # Even if there is an error, ensure the PDF-level data is logged
        append_to_master_csv({}, keywords, anti_keywords, score, 'error_pdf', url_html, pdf_path, pdf_url)
# Function to extract links from PDF annotations
def extract_links_from_pdf(reader):
    output = {}
    try:
        for page_num, page in enumerate(reader.pages):
            output[page_num] = []
            if '/Annots' in page:
                annotations = page['/Annots']
                for annotation in annotations:
                    annotation_obj = annotation.get_object()
                    if annotation_obj and isinstance(annotation_obj, dict):
                        if '/A' in annotation_obj and '/URI' in annotation_obj['/A']:
                            uri = annotation_obj['/A']['/URI']
                            # Extract the rectangle if available
                            rect = annotation_obj.get('/Rect', None)
                            if rect:
                                output[page_num].append({
                                    'page': page_num + 1,
                                    'uri': uri,
                                    'rect': rect
                                })
                            else:
                                # Capture the URL even without a /Rect
                                output[page_num].append({
                                    'page': page_num + 1,
                                    'uri': uri,
                                    'rect': None  # No rectangle available
                                })
    except Exception as e:
        logging.error(f"Failed to extract links from PDF: {e}")
    return output

# Function to search for keywords in the text of a PDF
def contains_keywords(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            text = page.extract_text()
            final_score, keywords, anti_keywords = relative_keywords_score(text)
            if final_score > 0:
                return True, final_score, keywords, anti_keywords
    except Exception as e:
        logging.error(f"Failed to read PDF {pdf_path}: {e}")
    return False, 0, None, None

# Function to delete the PDF file and associated CSVs
def delete_files(pdf_path):
    try:
        os.remove(pdf_path)
        print(f"Deleted PDF: {pdf_path}")
        
        csv_file_base = os.path.basename(pdf_path)
        for csv_file in os.listdir(CSV_OUTPUT_DIR):
            if csv_file.startswith(csv_file_base):
                os.remove(os.path.join(CSV_OUTPUT_DIR, csv_file))
                print(f"Deleted CSV: {csv_file}")
    except Exception as e:
        logging.error(f"Failed to delete files for {pdf_path}: {e}")

# Ensure every step of the way, unmatched URLs and rows without URLs are logged
def append_to_master_csv(row, keywords, anti_keywords, score, content_type, url_html, pdf_local_file, pdf_url):
    # Append all relevant fields for both matching rows and pages
    data = {
        "Programme": row.get("Programme", "N/A"),
        "Service": row.get("Service", "N/A"),
        "Date of Transmission": row.get("Date of Transmission", "N/A"),
        "Issue": row.get("Issue", "N/A"),
        "Outcome": row.get("Outcome", "N/A"),
        "keywords": ', '.join(keywords) if keywords else "N/A",
        "anti-keywords": ', '.join(anti_keywords) if anti_keywords else "N/A",
        "Score": score,
        "type (HTML/PDF)": content_type,
        "url html": url_html if url_html else "N/A",
        "url Pdf": pdf_url if pdf_url else "N/A",
        "URL": row.get("URL", "N/A"),
        "Response": row.get("Response", "N/A"),
        "Complaint": row.get("Complaint", "N/A"),
        "Voice Verification": "N/A",  # Placeholder for future voice verification logic
    }
    
    # Add to master CSV
    df = pd.DataFrame([data])
    df.to_csv(MASTER_CSV_OUTPUT, mode='a', header=False, index=False)
    print(f"Appended to master CSV: {data}")

# Main function to coordinate the entire process
def main():
    page_number = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            pdf_links, url_html = process_page(page_number)
            if not pdf_links:
                break
            future_to_url = {executor.submit(download_pdf, pdf_url, url_html): pdf_url for pdf_url in pdf_links}
            
            for future in as_completed(future_to_url):
                pdf_path, url_html, pdf_url = future.result()
                if pdf_path:
                    contains, score, keywords, anti_keywords = contains_keywords(pdf_path)
                    if contains:
                        executor.submit(extract_tables_from_pdf, pdf_path, url_html,score, keywords, anti_keywords, pdf_url)
                    else:
                        delete_files(pdf_path)
            page_number += 1
            time.sleep(2)

if __name__ == "__main__":
    main()
