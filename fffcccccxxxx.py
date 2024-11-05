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


def clean_and_assign_headers(df, expected_headers):
    df = clean_newlines(df)
    num_columns = len(df.columns)
    num_expected = len(expected_headers)

    if num_columns <= num_expected:
        df.columns = expected_headers[:num_columns]
    else:
        df.columns = expected_headers + [f'Extra_Column_{i+1}' for i in range(num_columns - num_expected)]
    
    return df


def clean_newlines(df):
    return df.replace('\n', ' ', regex=True)


# Fallback function for extracting complaints from unstructured text
# Enhanced fallback function for extracting complaints from unstructured text
def extract_complaints(text):
    complaints = []
    
    # Improved regex patterns to cover different complaint structures
    programme_re = re.compile(r"(Programme|Show|Title):\s*([\w\s\W]+?)(?=Service|Channel|Date|Complaint|Outcome|$)")
    service_re = re.compile(r"(Service|Channel):\s*([\w\s\W]+?)(?=Date|Complaint|Issue|Outcome|$)")
    date_re = re.compile(r"(Date of Transmission|Broadcast Date):\s*(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)")
    issue_re = re.compile(r"(Complaint|Issue|Problem):\s*([\w\s\W]+?)(?=Outcome|$)")
    outcome_re = re.compile(r"(Outcome|Resolution):\s*([\w\s\W]+?)(?=$)")

    # Iterate through text lines to extract each complaint field
    for line in text.splitlines():
        programme = programme_re.search(line)
        service = service_re.search(line)
        date = date_re.search(line)
        issue = issue_re.search(line)
        outcome = outcome_re.search(line)

        # If any of the fields match, construct complaint data dictionary
        if programme or service or date or issue or outcome:
            complaint = {
                "Programme": programme.group(2).strip() if programme else "N/A",
                "Service": service.group(2).strip() if service else "N/A",
                "Date of Transmission": date.group(2).strip() if date else "N/A",
                "Issue": issue.group(2).strip() if issue else "N/A",
                "Outcome": outcome.group(2).strip() if outcome else "N/A",
            }
            complaints.append(complaint)
            logging.info(f"Extracted complaint from unstructured text: {complaint}")
            
    return complaints if complaints else None


# Function to attempt reading tables or fallback to text extraction
def extract_tables_or_text_from_pdf(pdf_file):
    try:
        tables = camelot.read_pdf(pdf_file, pages="all", flavor="lattice") # or camelot.read_pdf(pdf_file, pages="all", flavor="stream")
        if tables.n == 0:
            raise Exception("No tables found in PDF.")
        return tables if tables else None
    except Exception as e:
        logging.error(f"Failed to extract tables from {pdf_file}: {e}")
        with open(pdf_file, "rb") as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages)
        logging.info(f"No tables found, extracting complaints from text in {pdf_file}.")
        return extract_complaints(text)


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

    def extract_information_html(self,soup, url):
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

        # Extract Programme title from the h1 tag within #block-bbc-contact-page-title
        title = soup.select_one("#block-bbc-contact-page-title h1")
        if title:
            data = title.text.strip().split(", ")
            extracted_data["Programme"] =  data[0] if len(data) > 0 else "N/A"
            extracted_data["Service"] =  data[0] if len(data) >1 else "N/A"
            extracted_data["Date of Transmission"] =  data[0] if len(data) >2 else "N/A"
            

        # Extract Date of Transmission using the datetime attribute from the time tag
        date_transmission = soup.select_one(".published-date time")
        if date_transmission and date_transmission.get("datetime"):
            extracted_data["Date of Transmission"] = date_transmission.get("datetime")

        # Extract Complaint Issue by selecting the relevant section under .ecu-complaint
        complaint = soup.select_one(".ecu-complaint div p")
        if complaint:
            extracted_data["Issue"] = complaint.text.strip()

        # Extract Outcome by selecting the relevant section under .ecu-outcome
        outcome = soup.select_one(".ecu-outcome div p")
        if outcome:
            extracted_data["Outcome"] = outcome.text.strip()

        return extracted_data


    # Function to scrape the content from a web page
    def _scrape_web_content(self, full_url):
        try:
            response = requests.get(full_url, timeout=60, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            text = " ".join([p.text for p in soup.find_all("p")])
            return text, soup  
        except requests.RequestException as e:
            logging.error(f"Failed to scrape {full_url}: {e}")
            return "", None


    def process_item(self, item, url_html, category_name):
        pdf_url = item.get("pdf_link")
        if pdf_url and ('.pdf') in pdf_url.lower():

            print(pdf_url)
            pdf_file = self._download_pdf(pdf_url)
            if pdf_file:
                self.extract_from_pdf(pdf_file, url_html, pdf_url, item.get("time"), category_name)
            return

        full_url = self.get_full_url(url_html, item.get("link"))
        content, soup = self._scrape_web_content(full_url)
        if not soup:
            return
        
        row = self.extract_information_html(soup, full_url)
        if content:
            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(content.replace("  ", " "))
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


    def extract_from_pdf(self, pdf_file, url_html, pdf_url, time_item, category):
        try:
            tables_or_complaints = extract_tables_or_text_from_pdf(pdf_file)

            # If the PDF has tables, process them
            if isinstance(tables_or_complaints, camelot.core.TableList):  # Camelot table object
                for table in tables_or_complaints:
                    cleaned_df = clean_and_assign_headers(table.df, self.EXPECTED_HEADERS)
                    for index, row in cleaned_df.iterrows():
                        row_text = ' '.join(map(str, row.values))
                        score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(row_text.replace("  ", " "))
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
                                "URL": pdf_url,
                                "time": time_item,
                                "item_time": "Unknown",
                                "category": category,
                                "Page": table.page,
                                "row_index": index,
                            })

            # If no tables were found, fall back to extracting free-form text
            else:
                with open(pdf_file, "rb") as f:
                    reader = PdfReader(f)
                    text = " ".join(page.extract_text() for page in reader.pages if page.extract_text())
                    complaints = extract_complaints(text)  # Extract complaints from text

                    if complaints:
                        for complaint in complaints:
                            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(
                                " ".join(complaint.values()).replace("  ", " ")
                            )
                            if score > 0:
                                self._append_to_master_csv({
                                    **complaint,
                                    "keywords": ", ".join(keywords),
                                    "Score": score,
                                    "type (HTML/PDF)": "PDF",
                                    "url html": url_html,
                                    "url Pdf": pdf_url,
                                    "URL": pdf_url,
                                    "time": time_item,
                                    "item_time": "Unknown",
                                    "category": category,
                                    "Page": "All Pages",
                                    "row_index": "N/A",
                                })

        except Exception as e:
            logging.error(f"Failed to process {pdf_file}: {e}")

    def _download_pdf(self, pdf_url, retries=3):
        local_filename = os.path.join(self.PDF_DIR, unquote(os.path.basename(pdf_url)))
        logging.info(f"Attempting to download PDF: {pdf_url}")
        for attempt in range(retries):
            try:
                response = requests.get(pdf_url, timeout=10, headers=headers)
                response.raise_for_status()
                with open(local_filename, "wb") as f:
                    f.write(response.content)
                return local_filename
            except requests.RequestException as e:
                logging.warning(f"Retry {attempt + 1}/{retries} failed for {pdf_url}: {e}")
                time.sleep(2)
        logging.error(f"Failed to download {pdf_url} after {retries} attempts")
        return None

    def get_full_url(self, base_url, relative_url):
        if not relative_url:
            return None
        if relative_url.startswith("http"):
            full_url = relative_url
        else:
            full_url = urljoin(base_url, relative_url)
        return full_url if self.is_valid_bbc_url(full_url) else None

    def is_valid_bbc_url(self, url):
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith(("bbc.co.uk", "bbc.com"))

    def _append_to_master_csv(self, row_data):
        with self.csv_lock:
            pd.DataFrame([row_data]).to_csv(self.MASTER_CSV_OUTPUT, mode="a", header=False, index=False)

    # Function to process a page of complaint items
    def process_page(self, page_number, base_url, category_name):
        # Format the URL with the page number
        url = base_url.format(page_number)

        # # Limit the number of pages to process
        # if page_number > 100:
        #     return [], None


        try:
            # Send the GET request
            response = requests.get(url, timeout=10, headers=headers)
            if response.status_code == 404:
                return [], None
            response.raise_for_status()

            # Sleep to avoid overwhelming the server
            time.sleep(2)

            # Parse the HTML content
            soup = BeautifulSoup(response.content, "html.parser")
            tile_items = soup.select(".tile")  # Locate each tile item

            # Initialize variables for links and seen links
            links = []
            seen_links = set()

            # # Minimum tiles expected to proceed
            # if len(tile_items) < 1:
            #     return [], None

            for z in tile_items:
                # Initialize dictionary for storing item details
                item = {}
                if "white-bg" not in z.get("class", []):  # Fixed here
                    continue
                
                # Extract time information if available
                time_elem = z.select_one("time")
                if time_elem:
                    item["time"] = time_elem.get("datetime")

                # Extract links from each tile item
                item_links = z.find_all("a")
                for link in item_links:
                    full_link = self.get_full_url(url, link.get("href"))

                    # Avoid duplicate links
                    if full_link in seen_links:
                        continue
                    seen_links.add(full_link)

                    # Store the PDF or HTML link depending on type
                    if full_link and "bbc.co.uk" in full_link:
                        if "pdf" in full_link:
                            item["pdf_link"] = full_link
                        else:
                            item["link"] = full_link

                # Append item to links if it has a valid link
                if item and (item.get("link") or item.get("pdf_link")):
                    links.append(item)

            return links, url

        except requests.RequestException as e:
            logging.error(f"Failed to process page {page_number}: {e}")
            return [], None



    def main(self):
        categories = {
            "recent-ecu": "https://www.bbc.co.uk/contact/recent-ecu?page={}",
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
