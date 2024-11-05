import difflib
import threading
from urllib.parse import urlparse, urljoin, unquote
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import camelot
import re

from utils.keywords_finder import KeypaceFinder
headers= {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-GPC": "1",
    "If-Modified-Since": "Fri, 17 May 2024 11:07:07 GMT",
    "If-None-Match": "\"66473a5b-6c734\"",
    "Priority": "u=0, i"
}
EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]

# Function to clean newline characters from the data
def clean_newlines(df):
    return df.map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

# Function to check if two bounding boxes overlap
def is_bbox_overlap(bbox1, bbox2):
    x1_1, y1_1, x2_1, y2_1 = bbox1
    x1_2, y1_2, x2_2, y2_2 = bbox2
    return not (x2_1 < x1_2 or x2_2 < x1_1 or y2_1 < y1_2 or y2_2 < y1_1)

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


# Function to clean and assign headers to a table
def clean_and_assign_headers(df):
    df = clean_newlines(df)
    num_columns = len(df.columns)
    
    # If columns are less than expected, add placeholders
    if num_columns:
        logging.warning(f"Table has {num_columns} columns, but expected {len(EXPECTED_HEADERS)}.")
        missing_columns = len(EXPECTED_HEADERS) - num_columns
        for i in range(missing_columns):
            df[f'Column{num_columns + i}'] = None  # Fill missing columns with None or 'N/A'
    
    # Slice the expected headers to match the actual number of columns
    df.columns = EXPECTED_HEADERS[:num_columns]
    
    # Ensure any additional columns beyond expected are labeled generically
    if num_columns > len(EXPECTED_HEADERS):
        for extra_col in range(len(EXPECTED_HEADERS), num_columns):
            df.columns = list(df.columns) + [f'Extra_Column_{extra_col - len(EXPECTED_HEADERS) + 1}']
    
    return df



# Function to try matching unmatched URLs
def match_unmatched_urls(cleaned_df, unmatched_links):
    c = set()
    unmatched_links_temp = unmatched_links.copy()
    for url in unmatched_links:
        if url in c:
            continue
        c.add(url)
        print(f"Processing unmatched URL: {url}")
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


# Function to match URL to row based on bounding boxes
def match_url_to_row(table, pdf_links):
    page_links = pdf_links.get(table.page - 1, [])
    matched_links = []
    unmatched_links = set()
    output = []

    for row_id, rows in enumerate(table.cells):
        matched = False  # Initialize matched flag for the row
        for cell in rows:
            cell_bbox = (cell.x1, cell.y1, cell.x2, cell.y2)
            for link in page_links:
                if link and isinstance(link, dict) and 'rect' in link and 'uri' in link:
                    link_bbox = (
                        link['rect'][0],
                        link['rect'][1],
                        link['rect'][2],
                        link['rect'][3]
                    )
                    if is_bbox_overlap(cell_bbox, link_bbox):
                        matched_links.append((cell_bbox, link['uri']))
                        print(f"Matched link {link['uri']} with cell bounding box {cell_bbox}")
                        output.append({
                            "row_id": row_id,
                            "cell_bbox": cell_bbox,
                            "link": link['uri']
                        })
                        matched = True
                        break  # Exit the link loop once a match is found
                else:
                    logging.warning(f"Skipping invalid or undefined link data: {link}")
            if matched:
                break  # Exit the cell loop once a match is found

        if not matched:
            # No link matched for this row; add all unmatched URIs from page_links
            for link in page_links:
                if link and 'uri' in link:
                    unmatched_links.add(link['uri'])
                else:
                    unmatched_links.add(None)

    return matched_links, list(unmatched_links), output



class BBCComplaintScraper:
    def __init__(self):
        self.keypaceFinder = KeypaceFinder()
        self.PDF_DIR = "data/bbc_complaint_pdfs"
        self.CSV_OUTPUT_DIR = "data/data/csv_outputs"
        self.MASTER_CSV_OUTPUT = "data/bbc_transphobia_complaint_service_reports_keywords_matched.csv"
        os.makedirs(self.PDF_DIR, exist_ok=True)
        os.makedirs(self.CSV_OUTPUT_DIR, exist_ok=True)
        
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        
        self.EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]
        self.csv_lock = threading.Lock()
        
        # Initialize Master CSV
        if not os.path.exists(self.MASTER_CSV_OUTPUT):
            master_headers = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome",
                              "keywords", "anti-keywords", "Score", "type (HTML/PDF)", "url html", "url Pdf", 
                              "URL", "time", "item_time", "category", "Page", "row_index"]
            pd.DataFrame(columns=master_headers).to_csv(self.MASTER_CSV_OUTPUT, index=False)



    def process_page(self,page_number, base_url, category_name):
        url = base_url.format(page_number)
        if page_number > 100:
            return [], None
        logging.info(f"Processing page {page_number} for category: {category_name}")
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 404:
                return [], None
            response.raise_for_status()

            time.sleep(2)
            soup = BeautifulSoup(response.content, "html.parser")
            tile_items = soup.select(".tile")
            links = []
            if len(tile_items) < 4:
                return [], None
            seen_links = set()
            for z in tile_items:
                item = {}
                if "white-bg" not in z.get("class"):
                    continue
                time_elem = z.select_one("time")
                if time_elem:
                    item["time"] = time_elem.get("datetime")

                item_links = z.find_all("a")
                for link in item_links:
                    full_link = self.get_full_url(url, link.get("href"))
                    if full_link in seen_links:
                        continue
                    seen_links.add(full_link)
                    if full_link and "bbc.co.uk" in full_link:
                        if "pdf" in full_link:
                            item["pdf_link"] = full_link
                        else:
                            item["link"] = full_link
                if item and (item.get("link") or item.get("pdf_link")):
                    links.append(item)

            return links, url
        except requests.RequestException as e:
            logging.error(f"Failed to process page {page_number}: {e}")
        return [], None

    def _is_valid_bbc_url(self, url):
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith(("bbc.co.uk", "bbc.com"))

    def is_valid_bbc_url(self,url):
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith("bbc.co.uk") or parsed_url.netloc.endswith("bbc.com") or parsed_url.netloc.endswith("bbci.co.uk")
    
    def _download_pdf(self, pdf_url, retries=3):
        local_filename = os.path.join(self.PDF_DIR, unquote(os.path.basename(pdf_url)))
        logging.info(f"Attempting to download PDF: {pdf_url}")
        for attempt in range(retries):
            try:
                response = requests.get(pdf_url, timeout=10,headers=headers)
                response.raise_for_status()
                with open(local_filename, "wb") as f:
                    f.write(response.content)
                return local_filename
            except requests.RequestException as e:
                logging.warning(f"Retry {attempt + 1}/{retries} failed for {pdf_url}: {e}")
                time.sleep(2)
        logging.error(f"Failed to download {pdf_url} after {retries} attempts")
        return None

    def _scrape_web_content(self, full_url):
        try:
            logging.info(f"Scraping web content from URL: {full_url}")
            response = requests.get(full_url, timeout=60,headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            text = " ".join([p.text for p in soup.find_all("p")])
            return text, soup
        except requests.RequestException as e:
            logging.error(f"Failed to scrape {full_url}: {e}")
            return "", None

    def _extract_links_from_pdf(self, reader):
        output = {}
        for page_num, page in enumerate(reader.pages):
            output[page_num] = []
            if "/Annots" in page:
                for annotation in page["/Annots"]:
                    obj = annotation.get_object()
                    if obj and "/A" in obj and "/URI" in obj["/A"]:
                        uri = obj["/A"]["/URI"]
                        rect = obj.get("/Rect")
                        output[page_num].append({"page": page_num + 1, "uri": uri, "rect": rect})
        return output

    def _append_to_master_csv(self, row_data):
        with self.csv_lock:
            pd.DataFrame([row_data]).to_csv(self.MASTER_CSV_OUTPUT, mode="a", header=False, index=False)



    def extract_information_html(self, soup, url):
        """
        Extracts information from the HTML content of a complaint page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content of the page.
            url (str): The URL of the page being processed.
        
        Returns:
            dict: A dictionary containing extracted information with keys like 
                  "Programme," "Service," "Date of Transmission," "Issue," and "Outcome".
        """
        extracted_data = {
            "Programme": "N/A",
            "Service": "N/A",
            "Date of Transmission": "N/A",
            "Issue": "N/A",
            "Outcome": "N/A"
        }

        # Define the CSS selectors or element paths based on HTML structure
        programme = soup.select_one(".programme")  # Replace with actual CSS selector for Programme
        service = soup.select_one(".service")      # Replace with actual CSS selector for Service
        date_transmission = soup.select_one(".date-transmission")  # Replace with actual selector
        issue = soup.select_one(".issue")
        outcome = soup.select_one(".outcome")

        # Assign text content to each field if it exists
        extracted_data["Programme"] = programme.text.strip() if programme else "N/A"
        extracted_data["Service"] = service.text.strip() if service else "N/A"
        extracted_data["Date of Transmission"] = date_transmission.text.strip() if date_transmission else "N/A"
        extracted_data["Issue"] = issue.text.strip() if issue else "N/A"
        extracted_data["Outcome"] = outcome.text.strip() if outcome else "N/A"

        return extracted_data
    
    def get_full_url(self,base_url, relative_url):
        if not relative_url:
            return None
        if relative_url.startswith("http"):
            full_url = relative_url
        else:
            full_url = urljoin(base_url, relative_url)
        if self.is_valid_bbc_url(full_url):
            return full_url
        else:
            logging.info(f"Skipping non-BBC URL: {full_url}")
            return None

    
    def process_item(self, item, url_html, category_name):
        pdf_url = item.get("pdf_link")
        if pdf_url and "pdf" in pdf_url:
            pdf_file = self._download_pdf(pdf_url)
            if pdf_file:
                self.extract_tables_from_pdf(pdf_file, url_html, pdf_url, item.get("time"), category_name)
            return

        full_url = self.get_full_url(url_html, item.get("link"))
        content, soup = self._scrape_web_content(full_url)
        if not soup:
            return
        
        row = self.extract_information_html(soup, full_url)
        if content:
            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(content)
            if score > 0:
                self._append_to_master_csv({
                    "Programme": row.get("Programme", "N/A"),
                    "Service": row.get("Service", "N/A"),
                    "Date of Transmission": row.get("Date of Transmission", "N/A"),
                    "Issue": row.get("Issue", "N/A"),
                    "Outcome": row.get("Outcome", "N/A"),
                    "keywords": ", ".join(keywords),
                    "Score": score,
                    "type (HTML/PDF)": "HTML",
                    "url html": url_html,
                    "URL": full_url,
                    "time": item.get("time"),
                    "category": category_name,
                    "item_time": "Unknown",
                    "Page": "N/A",
                    "row_index": "N/A",
                })

    def extract_tables_from_pdf(self, pdf_file, url_html, pdf_url, time_item, category):
        try:
            logging.info(f"Extracting tables from PDF: {pdf_file}")
            reader = PdfReader(pdf_file)
            pdf_links = self._extract_links_from_pdf(reader)

            # Attempt to read tables using Camelot in stream mode
            tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream")

            # Retry with lattice mode if no tables found or column mismatch
            if not tables:
                logging.warning(f"Retrying {pdf_file} with lattice mode due to insufficient columns.")
                tables = camelot.read_pdf(pdf_file, pages="all", flavor="lattice")

            if not tables:
                logging.info(f"No tables found in {pdf_file}")
                return  # Exit if no tables are found

            for table_num, table in enumerate(tables, start=1):
                # Clean and assign headers, handling single-column tables as well
                cleaned_df = clean_and_assign_headers(table.df)
                cleaned_df['URL'] = None

                # Skip processing if table columns still don’t match expected headers
                if len(cleaned_df.columns) < len(EXPECTED_HEADERS):
                    logging.warning(f"Skipping table in {pdf_file}: insufficient columns after retry.")
                    continue

                # Get matched and unmatched links for each table row
                matched_links, unmatched_links, matched_links_row_info = match_url_to_row(table, pdf_links) or ([], [], [])

                # Assign URLs to matched rows
                for link_info in matched_links_row_info:
                    row_id = link_info['row_id']
                    url = link_info['link']
                    if row_id < len(cleaned_df):
                        cleaned_df.at[row_id, 'URL'] = url

                # Match any remaining unmatched URLs
                cleaned_df, remaining_unmatched_links = match_unmatched_urls(cleaned_df, unmatched_links)

                # Process each row in cleaned_df for scoring
                for index, row in cleaned_df.iterrows():
                    row_text = ' '.join(map(str, row.values))
                    score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(row_text)
                    if score > 0:
                        self._append_to_master_csv({
                            "Programme": row.get("Programme", "N/A"),
                            "Service": row.get("Service", "N/A"),
                            "Date of Transmission": row.get("Date of Transmission", "N/A"),
                            "Issue": row.get("Issue", "N/A"),
                            "Outcome": row.get("Outcome", "N/A"),
                            "keywords": ", ".join(keywords),
                            "Score": score,
                            "type (HTML/PDF)": "PDF",
                            "url html": url_html,
                            "url Pdf": pdf_url,
                            "URL": row.get('URL', 'N/A'),
                            "time": time_item,
                            "item_time": "Unknown",
                            "category": category,
                            "Page": table.page,
                            "row_index": index,
                        })

        except Exception as e:
            logging.error(f"Error processing PDF {pdf_file}: {e}")




    def main(self):
        categories = {
            # "recent-ecu": "https://www.bbc.co.uk/contact/recent-ecu?page={}",
            "archived-ecu": "https://www.bbc.co.uk/contact/archived-ecu?page={}",
            "recent-complaints": "https://www.bbc.co.uk/contact/complaints/recent-complaints?page={}",
            "complaint-service-reports": "https://www.bbc.co.uk/contact/complaint-service-reports?page={}",
        }

        for category_name, base_url in categories.items():
            page_number = 0
            while True:
                items, url_html = self.process_page(page_number, base_url, category_name)
                if not items:
                    break
                for item in items:
                    self.process_item(item, url_html, category_name)
                page_number += 1

if __name__ == "__main__":
    scraper = BBCComplaintScraper()
    scraper.main()
