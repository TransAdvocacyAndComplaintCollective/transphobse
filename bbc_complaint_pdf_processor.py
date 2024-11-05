import difflib
import threading
from urllib.parse import urlparse, urljoin, unquote
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import os
import logging
import time
import pandas as pd
import camelot
import re

from utils.keywords_finder import KeypaceFinder

headers = {
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

class BBCComplaintScraper:
    EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]

    def __init__(self):
        self.keypaceFinder = KeypaceFinder()
        self.PDF_DIR = "data/bbc_complaint_pdfs"
        self.CSV_OUTPUT_DIR = "data/data/csv_outputs"
        self.MASTER_CSV_OUTPUT = "data/bbc_transphobia_complaint_service_reports_keywords_matched.csv"
        self.UNMATCHED_URLS_OUTPUT_DIR = "data/unmatched_urls"
        os.makedirs(self.PDF_DIR, exist_ok=True)
        os.makedirs(self.CSV_OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.UNMATCHED_URLS_OUTPUT_DIR, exist_ok=True)
        
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        
        self.csv_lock = threading.Lock()
        self.c = set()  # For tracking processed URLs
        
        # Initialize Master CSV
        if not os.path.exists(self.MASTER_CSV_OUTPUT):
            master_headers = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome",
                              "keywords", "anti-keywords", "Score", "type (HTML/PDF)", "url html", "url Pdf", 
                              "URL", "time", "item_time", "category", "Page", "row_index"]
            pd.DataFrame(columns=master_headers).to_csv(self.MASTER_CSV_OUTPUT, index=False)

    def clean_newlines(self, df):
        return df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
    
    def is_bbox_overlap(self, bbox1, bbox2):
        x1_1, y1_1, x2_1, y2_1 = bbox1
        x1_2, y1_2, x2_2, y2_2 = bbox2
        return not (x2_1 < x1_2 or x2_2 < x1_1 or y2_1 < y1_2 or y2_2 < y1_1)
    
    def scrape_web_content(self, url):
        try:
            response = requests.get(url, timeout=60, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            return ' '.join([p.text for p in soup.find_all('p')])
        except requests.RequestException as e:
            logging.error(f"Failed to scrape {url}: {e}")
            return ""
    
    def clean_and_assign_headers(self, df):
        df = self.clean_newlines(df)
        if len(df.columns) != len(self.EXPECTED_HEADERS):
            logging.warning(f"Table has {len(df.columns)} columns, but expected {len(self.EXPECTED_HEADERS)}")
            df.columns = ['Column' + str(i) for i in range(len(df.columns))]
        else:
            df.columns = self.EXPECTED_HEADERS
        return df
    
    def match_unmatched_urls(self, cleaned_df, unmatched_links):
        unmatched_links_temp = unmatched_links.copy()
        for url in unmatched_links:
            if url in self.c:
                continue
            self.c.add(url)
            best_match_row = None
            best_match_score = 0
            url_text = url.lower()
            for index, row in cleaned_df.iterrows():
                row_text = ' '.join(map(str, [row.get("Programme", ""), row.get("Service", ""), row.get("Date of Transmission", "")])).lower()
                match_score = difflib.SequenceMatcher(None, url_text, row_text).ratio()
                if match_score > best_match_score and match_score > 0.5:
                    best_match_row = index
                    best_match_score = match_score
            if best_match_row is not None and pd.isna(cleaned_df.at[best_match_row, 'URL']):
                cleaned_df.at[best_match_row, 'URL'] = url
                unmatched_links_temp.remove(url)
        return cleaned_df, unmatched_links_temp
    
    def match_url_to_row(self, table, pdf_links):
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
                        if self.is_bbox_overlap(cell_bbox, link_bbox):
                            matched_links.append((cell_bbox, link['uri']))
                            output.append({"row_id": row_id, "cell_bbox": cell_bbox, "link": link['uri']})
                            matched = True
                            break
                if not matched and link and 'uri' in link:
                    unmatched_links.add(link['uri'])
        return matched_links, unmatched_links, output
    
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
        header = soup.find("h1", class_="gel-trafalgar")
        complaint = soup.find("div", class_="ecu-complaint")
        outcome = soup.find("div", class_="ececu-outcome")
        extracted_data = {
            "Programme": header.get_text(strip=True) if header else "N/A",
            "Service": "N/A",
            "Date of Transmission": "N/A",
            "Issue": complaint.get_text(strip=True) if complaint else "N/A",
            "Outcome": outcome.get_text(strip=True) if outcome else "N/A"
        }
        return extracted_data
    
    def extract_tables_from_pdf(self, pdf_file, url_html, pdf_url, time_item, category):
        try:
            logging.info(f"Extracting tables from PDF: {pdf_file}")
            reader = PdfReader(pdf_file)
            pdf_links = self._extract_links_from_pdf(reader)
            tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream")
            if tables:
                for table in tables:
                    matched_links, unmatched_links, matched_links_row_info = self.match_url_to_row(table, pdf_links)
                    cleaned_df = self.clean_and_assign_headers(table.df)
                    cleaned_df['URL'] = None
                    for link_info in matched_links_row_info:
                        row_id = link_info['row_id']
                        url = link_info['link']
                        if row_id < len(cleaned_df):
                            cleaned_df.at[row_id, 'URL'] = url
                    cleaned_df, remaining_unmatched_links = self.match_unmatched_urls(cleaned_df, unmatched_links)
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
                                "anti-keywords": ", ".join(anti_keywords),
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
    
    def get_full_url(self, base_url, relative_url):
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
    
    def is_valid_bbc_url(self, url):
        parsed_url = urlparse(url)
        return parsed_url.netloc.endswith(("bbc.co.uk", "bbc.com", "bbci.co.uk"))
    def _scrape_web_content(self, full_url):
        """
        Scrapes web content from the given URL, returning the combined text content and a BeautifulSoup object.
        """
        try:
            logging.info(f"Scraping web content from URL: {full_url}")
            response = requests.get(full_url, timeout=60, headers=headers)
            response.raise_for_status()  # Raise an error for HTTP issues

            # Parse the page content
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract text from paragraphs
            text = " ".join([p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)])

            # Log warning if no text was found
            if not text:
                logging.warning(f"No text content found on {full_url}")

            return text, soup  # Return both the plain text and parsed BeautifulSoup object

        except requests.Timeout:
            logging.error(f"Timeout occurred when trying to scrape {full_url}")
            return "", None  # Return empty text and None for soup

        except requests.RequestException as e:
            logging.error(f"Request failed for {full_url}: {e}")
            return "", None  # Return empty text and None for soup

    def process_item(self, item, url_html, category_name):
        """
        Process a single complaint item by extracting and analyzing content from both HTML and PDF links.
        """
        # Extract the main URL (HTML) and PDF URL if available
        html_link = self.get_full_url(url_html, item.get("link"))
        pdf_link = self.get_full_url(url_html, item.get("pdf_link"))
        item_time = item.get("time", "Unknown")

        # Process the HTML link if it exists
        if html_link:
            content, soup = self._scrape_web_content(html_link)
            if soup:
                row = self.extract_information_html(soup, html_link)
                if content:
                    score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(content)
                    if score > 0:
                        logging.info(f"Appending HTML data for {html_link}")
                        self._append_to_master_csv({
                            "Programme": row.get("Programme", "N/A"),
                            "Service": row.get("Service", "N/A"),
                            "Date of Transmission": row.get("Date of Transmission", "N/A"),
                            "Issue": row.get("Issue", "N/A"),
                            "Outcome": row.get("Outcome", "N/A"),
                            "keywords": ", ".join(keywords),
                            "Score": score,
                            "type (HTML/PDF)": "HTML",
                            "url html": html_link,
                            "url Pdf": "N/A",
                            "URL": html_link,
                            "time": item_time,
                            "item_time": item_time,
                            "category": category_name,
                            "Page": "N/A",
                            "row_index": "N/A",
                        })

        # Process the PDF link if it exists
        if pdf_link:
            pdf_file = self._download_pdf(pdf_link)
            if pdf_file:
                logging.info(f"Processing PDF {pdf_file}")
                self.extract_tables_from_pdf(pdf_file, html_link, pdf_link, item_time, category_name)

    def process_page(self, page_number, base_url, category_name):
        url = base_url.format(page_number)
        if page_number > 100:
            return [], None
        logging.info(f"Processing page {page_number} for category: {category_name}")
        try:
            response = requests.get(url, timeout=10, headers=headers)
            if response.status_code == 404:
                return [], None
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            tile_items = soup.select(".tile")
            links = []
            seen_links = set()
            for tile in tile_items:
                if "white-bg" not in tile.get("class", []):
                    continue
                item = {"time": tile.select_one("time").get("datetime") if tile.select_one("time") else None}
                for link in tile.find_all("a"):
                    full_link = self.get_full_url(url, link.get("href"))
                    if full_link and "bbc.co.uk" in full_link and full_link not in seen_links:
                        seen_links.add(full_link)
                        if "pdf" in full_link:
                            item["pdf_link"] = full_link
                        else:
                            item["link"] = full_link
                if item.get("link") or item.get("pdf_link"):
                    links.append(item)
            return links, url
        except requests.RequestException as e:
            logging.error(f"Failed to process page {page_number}: {e}")
        return [], None
    
    def main(self):
        categories = {
            "recent-ecu": "https://www.bbc.co.uk/contact/recent-ecu?page={}",
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
