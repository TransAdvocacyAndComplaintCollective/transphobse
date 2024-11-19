import difflib
import threading
from urllib.parse import urlparse, urljoin, unquote
import fitz
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
    "If-None-Match": '"66473a5b-6c734"',
    "Priority": "u=0, i",
}
EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]


# Function to clean newline characters from the data
def clean_newlines(df):
    return df.map(lambda x: x.replace("\n", " ") if isinstance(x, str) else x)


# Function to check if two bounding boxes overlap
from typing import Any, Dict, List, Tuple


def is_bbox_overlap(
    bbox1: Tuple[float, float, float, float], bbox2: Tuple[float, float, float, float]
) -> bool:
    """
    Checks if two axis-aligned bounding boxes (AABBs) overlap.

    Args:
        bbox1: (x1, y1, x2, y2) coordinates of the first bounding box.
        bbox2: (x1, y1, x2, y2) coordinates of the second bounding box.

    Returns:
        True if the bounding boxes overlap, False otherwise.
    """
    # Unpack bounding box coordinates
    x1_1, y1_1, x2_1, y2_1 = bbox1
    x1_2, y1_2, x2_2, y2_2 = bbox2

    # Check for zero-area boxes (invalid bounding boxes)
    if x1_1 >= x2_1 or y1_1 >= y2_1 or x1_2 >= x2_2 or y1_2 >= y2_2:
        return False

    # Check for overlap using direct conditions
    if x1_1 >= x2_2:  # bbox1 is completely to the right of bbox2
        return False
    if x2_1 <= x1_2:  # bbox1 is completely to the left of bbox2
        return False
    if y1_1 >= y2_2:  # bbox1 is completely above bbox2
        return False
    if y2_1 <= y1_2:  # bbox1 is completely below bbox2
        return False

    # Bounding boxes overlap
    return True


# Function to scrape the content from the web page pointed to by a URL
def scrape_web_content(url):
    if (
        url
        == "https://www.ofcom.org.uk/tv-radio-and-on-demand/broadcast-codes/broadcast-code"
    ):
        return ""
    try:
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            return ""
        response.raise_for_status()  # Check if the request was successful
        soup = BeautifulSoup(response.content, "html.parser")
        return " ".join([p.text for p in soup.find_all("p")])
    except requests.RequestException as e:
        logging.error(f"Failed to scrape {url}: {e}")
        return ""


# Function to clean and assign headers to a table
def clean_and_assign_headers(df):
    """
    Cleans the dataframe and assigns headers based on EXPECTED_HEADERS.

    Args:
        df (pd.DataFrame): The input dataframe to be cleaned and assigned headers.

    Returns:
        pd.DataFrame: Cleaned dataframe with assigned headers.
    """
    df = clean_newlines(df)

    num_columns = len(df.columns)
    expected_columns = len(EXPECTED_HEADERS)

    # If the DataFrame has fewer columns than expected, add placeholders
    if num_columns < expected_columns:
        logging.warning(
            f"Table has {num_columns} columns, but expected {expected_columns}. Adding placeholder columns."
        )
        for i in range(num_columns, expected_columns):
            df[f"Column_{i + 1}"] = None
        num_columns = expected_columns  # Update num_columns after adding placeholders

    # If the DataFrame has more columns than expected, rename the extra columns
    if num_columns > expected_columns:
        logging.warning(
            f"Table has {num_columns} columns, exceeding the expected {expected_columns}. Renaming extra columns."
        )
        extra_columns = [
            f"Extra_Column_{i - expected_columns + 1}"
            for i in range(expected_columns, num_columns)
        ]
        df.columns = EXPECTED_HEADERS + extra_columns
    else:
        df.columns = EXPECTED_HEADERS[:num_columns]

    # Truncate the DataFrame if it still has more columns than expected
    df = df.iloc[:, :expected_columns]

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
            row_text = " ".join(
                map(
                    str,
                    [
                        row.get("Programme", ""),
                        row.get("Service", ""),
                        row.get("Date of Transmission", ""),
                    ],
                )
            ).lower()
            match_score = difflib.SequenceMatcher(None, url_text, row_text).ratio()

            if match_score > best_match_score and match_score > 0.5:  # Threshold of 0.5
                best_match_row = index
                best_match_score = match_score

        if best_match_row is None:
            # Second attempt: Scrape the web page and match its content
            web_content = scrape_web_content(url)
            for index, row in cleaned_df.iterrows():
                row_text = " ".join(map(str, row)).lower()
                match_score = difflib.SequenceMatcher(
                    None, web_content, row_text
                ).ratio()

                if match_score > best_match_score and match_score > 0.5:
                    best_match_row = index
                    best_match_score = match_score

        if best_match_row is None:
            # Third attempt: Best effort fuzzy match
            for index, row in cleaned_df.iterrows():
                row_text = " ".join(map(str, row)).lower()
                match_score = difflib.SequenceMatcher(None, url_text, row_text).ratio()

                if match_score > best_match_score:
                    best_match_row = index
                    best_match_score = match_score

        # Assign the URL to the best matching row if found
        if best_match_row is not None and pd.isna(cleaned_df.at[best_match_row, "URL"]):
            cleaned_df.at[best_match_row, "URL"] = url
            unmatched_links_temp.remove(url)
    return cleaned_df, unmatched_links_temp


# Function to match URL to row based on bounding boxes
def match_url_to_row(
    table, pdf_links
) -> Tuple[
    List[Tuple[Tuple[float, float, float, float], str]], List[str], List[Dict[str, Any]]
]:
    """
    Matches URLs to table rows based on bounding boxes.

    Args:
        table: The table object containing cells with bounding boxes.
        pdf_links: A dictionary of page-wise links with bounding boxes.

    Returns:
        matched_links: List of tuples with cell bounding boxes and matched URIs.
        unmatched_links: List of unmatched URIs.
        output: List of dictionaries with matched row information.
    """
    page_links = pdf_links.get(table.page - 1, [])
    matched_links = []
    matched_uris = set()
    output = []
    unmatched_links = set(link["uri"] for link in page_links if "uri" in link)

    for row_id, rows in enumerate(table.cells):
        for cell in rows:
            cell_bbox = (cell.x1, cell.y1, cell.x2, cell.y2)

            for link in page_links:
                if not link or "rect" not in link or "uri" not in link:
                    logging.warning(f"Skipping invalid or undefined link data: {link}")
                    continue

                link_bbox = tuple(link["rect"])
                uri = link["uri"]

                # Check if the bounding boxes overlap
                if uri not in matched_uris and is_bbox_overlap(cell_bbox, link_bbox):
                    matched_links.append((cell_bbox, uri))
                    matched_uris.add(uri)
                    unmatched_links.discard(uri)

                    logging.info(
                        f"Matched link {uri} with cell bounding box {cell_bbox}"
                    )
                    output.append(
                        {"row_id": row_id, "cell_bbox": cell_bbox, "link": uri}
                    )
                    break  # Exit link loop once a match is found

    return matched_links, list(unmatched_links), output


master_headers = [
    "Programme",
    "Service",
    "Date of Transmission",
    "Issue",
    "Outcome",
    "keywords",
    "anti-keywords",
    "Score",
    "type (HTML/PDF)",
    "url html",
    "url Pdf",
    "URL",
    "time",
    "item_time",
    "category",
    "Page",
    "row_index",
]

# Regular expression patterns
title_pattern = re.compile(
    r"^(.*?),\s*(BBC [\w\s]+),\s*(\d{1,2} \w+ \d{4})", re.MULTILINE
)
complaint_pattern = re.compile(r"Complaint\s*\n(.*?)\nOutcome", re.DOTALL)
outcome_pattern = re.compile(
    r"Outcome\s*\n(.*?)\n(Further action|Resolved|Partly upheld|Upheld|Not upheld)",
    re.DOTALL,
)
further_action_pattern = re.compile(r"Further action\s*\n(.*?)(?=\n|$)", re.DOTALL)


def extract_info(pdf_path):
    results = []

    # Open the PDF file
    with fitz.open(pdf_path) as pdf:
        text = ""
        for page in pdf:
            text += page.get_text()

        # Split based on each complaint summary
        summaries = re.split(r"\n\n", text)

        # Extract information using regex
        for summary in summaries:
            title_match = title_pattern.search(summary)
            complaint_match = complaint_pattern.search(summary)
            outcome_match = outcome_pattern.search(summary)
            further_action_match = further_action_pattern.search(summary)

            if title_match:
                title, service, date = title_match.groups()
            else:
                continue

            complaint = complaint_match.group(1).strip() if complaint_match else "N/A"
            outcome = outcome_match.group(1).strip() if outcome_match else "N/A"
            further_action = (
                further_action_match.group(1).strip() if further_action_match else "N/A"
            )

            # Append the extracted data
            results.append(
                {
                    "Title": title,
                    "Service": service,
                    "Date": date,
                    "Complaint": complaint,
                    "Outcome": outcome,
                    "Further action": further_action,
                }
            )

    return results


class BBCComplaintScraper:
    def __init__(self):
        self.keypaceFinder = KeypaceFinder()
        self.PDF_DIR = "data/bbc_complaint_pdfs"
        self.CSV_OUTPUT_DIR = "data/data/csv_outputs"
        self.MASTER_CSV_OUTPUT = (
            "data/bbc_transphobia_complaint_service_reports_keywords_matched.csv"
        )
        os.makedirs(self.PDF_DIR, exist_ok=True)
        os.makedirs(self.CSV_OUTPUT_DIR, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.EXPECTED_HEADERS = [
            "Programme",
            "Service",
            "Date of Transmission",
            "Issue",
            "Outcome",
        ]
        self.csv_lock = threading.Lock()

        # Initialize Master CSV
        if not os.path.exists(self.MASTER_CSV_OUTPUT):
            pd.DataFrame(columns=master_headers).to_csv(
                self.MASTER_CSV_OUTPUT, index=False
            )

    def process_page(self, page_number, base_url, category_name):
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

    def is_valid_bbc_url(self, url):
        parsed_url = urlparse(url)
        return (
            parsed_url.netloc.endswith("bbc.co.uk")
            or parsed_url.netloc.endswith("bbc.com")
            or parsed_url.netloc.endswith("bbci.co.uk")
        )

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
                logging.warning(
                    f"Retry {attempt + 1}/{retries} failed for {pdf_url}: {e}"
                )
                time.sleep(2)
        logging.error(f"Failed to download {pdf_url} after {retries} attempts")
        return None


    def _scrape_web_content(self,full_url):
        try:
            logging.info(f"Scraping web content from URL: {full_url}")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
            }
            response = requests.get(full_url, timeout=60, headers=headers)
            if response.status_code != 200:
                logging.warning(f"Non-200 status code received: {response.status_code}")
                return "", None
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract all text content from the page
            text = soup.get_text(separator=" ", strip=True)

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
                        output[page_num].append(
                            {"page": page_num + 1, "uri": uri, "rect": rect}
                        )
        return output

    def _append_to_master_csv(self, row_data: dict):
        """
        Appends a row of data to the master CSV, ensuring the column order matches `master_headers`.

        Args:
            row_data (dict): The data row to append to the CSV file.
        """
        try:
            # Reorder the row data to match the master headers
            ordered_row = {key: row_data.get(key, "N/A") for key in master_headers}

            # Write the row to the CSV with a lock to prevent concurrent write issues
            with self.csv_lock:
                pd.DataFrame([ordered_row]).to_csv(
                    self.MASTER_CSV_OUTPUT, mode="a", header=False, index=False
                )

            logging.info("Row appended successfully to the master CSV.")
        except Exception as e:
            logging.error(f"Failed to append row to master CSV: {e}")

    def extract_information_html_2024(self, soup, url):
        # Initialize an empty list to store the results
        results = []

        # Find all <h2> tags which typically contain the section titles
        sections = soup.find_all("h2")

        # Loop through each section to extract the title, date, and correction text
        for section in sections:
            title = section.get_text(strip=True)
            correction_text = ""
            date = "No date found"

            # Extract the correction text (all paragraphs until an <em> tag is found)
            paragraph = section.find_next("p")
            while paragraph and not paragraph.find("em"):
                correction_text += paragraph.get_text(strip=True) + " "
                paragraph = paragraph.find_next("p")

            # Extract the date from the <em> tag if it exists
            if paragraph and paragraph.em:
                date = paragraph.em.get_text(strip=True)

            # Store the title, date, correction text, and the source URL
            results.append(
                {
                    "title": title,
                    "date": date,
                    "correction_text": correction_text.strip(),
                    "source_url": url,
                }
            )
            print(
                {
                    "title": title,
                    "date": date,
                    "correction_text": correction_text.strip(),
                    "source_url": url,
                })

        return results

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
        # Initialize a dictionary with default values
        extracted_data = {
            "Programme": "N/A",
            "Service": "N/A",
            "Date of Transmission": "N/A",
            "Issue": "N/A",
            "Outcome": "N/A",
        }

        def get_text(element):
            """Helper function to safely extract and clean text."""
            return element.get_text(strip=True).replace("\n"," ") if element else "N/A"

        # Extract Programme, Service, and Date of Transmission from title block
        programme = soup.select_one(".block-bbc-contact-page-title")
        if programme:
            title_parts = [part.strip() for part in programme.get_text().split(",")]
            extracted_data["Programme"] = title_parts[0] if len(title_parts) > 0 else "N/A"
            extracted_data["Service"] = title_parts[1] if len(title_parts) > 1 else "N/A"
            extracted_data["Date of Transmission"] = title_parts[2] if len(title_parts) > 2 else "N/A"

        # Extract Service (fallback method)
        service = soup.select_one(".service")
        extracted_data["Service"] = get_text(service)
        if not service:
            service = soup.select_one(".block-service")
            extracted_data["Service"] = get_text(service)
            
        # Extract Date of Transmission (fallback method)
        date_transmission = soup.select_one(".date-transmission")
        if extracted_data["Date of Transmission"] == "N/A":
            extracted_data["Date of Transmission"] = get_text(date_transmission)
        if not date_transmission:
            date_transmission = soup.select_one("time")
            extracted_data["Date of Transmission"] = get_text(date_transmission)

        # Extract Issue
        issue = soup.select_one(".issue")
        extracted_data["Issue"] = get_text(issue)
        if not issue:
            issue = soup.select_one(".ecu-complaint")
            extracted_data["Issue"] = get_text(issue)
        if not issue:
            issue = soup.select_one(".complaint-summary")
            extracted_data["Issue"] = get_text(issue)

        # Extract Outcome
        outcome = soup.select_one(".outcome")
        extracted_data["Outcome"] = get_text(outcome)
        if not outcome:
            outcome = soup.select_one(".ecu-outcome")
            extracted_data["Outcome"] = get_text(outcome)
        if not outcome:
            outcome = soup.select_one(".complaint-response")
            extracted_data["Outcome"] = get_text(outcome)


        # Additional validation and cleanup
        for key, value in extracted_data.items():
            if not value or value.isspace():
                extracted_data[key] = "N/A"

        return extracted_data

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

    def process_item(self, item, url_html, category_name):
        pdf_url = item.get("pdf_link")
        if pdf_url and "pdf" in pdf_url:
            pdf_file = self._download_pdf(pdf_url)
            if pdf_file:
                print(pdf_file, url_html, pdf_url, item.get("time"), category_name)
                self.extract_tables_from_pdf(
                    pdf_file, url_html, pdf_url, item.get("time"), category_name
                )
            return

        full_url = self.get_full_url(url_html, item.get("link"))
        content, soup = self._scrape_web_content(full_url)
        if not soup:
            return
        if full_url in "bbc.co.uk/helpandfeedback/corrections_clarifications":
            rows = self.extract_information_html_2024(soup, full_url)
            for row in rows:
                score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(
                    content
                )
                if score > 0:
                    self._append_to_master_csv(
                        {
                            "Programme": row.get("title", "N/A"),
                            "Service": row.get("Service", "N/A"),
                            "Date of Transmission": row.get("date", "N/A"),
                            "Issue": row.get("Issue", "N/A"),
                            "Outcome": row.get("Outcome", "N/A"),
                            "keywords": ", ".join(keywords),
                            "Score": score,
                            "url html": url_html,
                            "URL": full_url,
                            "time": item.get("correction_text"),
                        }
                    )
            if rows:
                return
        row = self.extract_information_html(soup, full_url)
        if content:
            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(
                content
            )
            if "trans " in content:
                print("content trans")
            if score > 0 :
                self._append_to_master_csv(
                    {
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
                    }
                )

    # def extract_tables_from_pdf(self, pdf_file, url_html, pdf_url, time_item, category):
    def extract_tables_from_pdf(self, pdf_file, url_html, pdf_url, time_item, category):
        try:
            tables = None
            logging.info(f"Extracting tables from PDF: {pdf_file}")
            reader = PdfReader(pdf_file)
            pdf_links = self._extract_links_from_pdf(reader)
            if  category != "recent-complaints":
                # Attempt to read tables using Camelot in stream mode
                tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream")

                # Retry with lattice mode if no tables found or column mismatch
                if not tables:
                    tables = camelot.read_pdf(pdf_file, pages="all", flavor="lattice")

            if not tables or category == "recent-complaints":
                c = extract_info(pdf_file)
                print(c)
                for page_num, page in enumerate(reader.pages):
                    page_text = page.extract_text()
                    score, keywords, anti_keywords = (
                        self.keypaceFinder.relative_keywords_score(page_text)
                    )
                    # row_text = " ".join(map(str, row2.values)).replace("\n", " ")
                    # score2, keywords2, anti_keywords = (
                    #     self.keypaceFinder.relative_keywords_score(row_text)
                    # )
                    # score = max(score2,score)
                    if score > 0:
                        self._append_to_master_csv(
                            {
                                "Programme": "N/A",
                                "Service": "N/A",
                                "Date of Transmission": "N/A",
                                "Issue": "N/A",
                                "Outcome": "N/A",
                                "keywords": ", ".join(keywords),
                                "Score": score,
                                "type (HTML/PDF)": "PDF",
                                "url html": url_html,
                                "url Pdf": pdf_url,
                                "URL": "",
                                "time": time_item,
                                "item_time": "Unknown",
                                "category": category,
                                "Page": page_num,
                                # "row_index": index,
                            }
                        )
                return  # Exit if no tables are found

            for table_num, table in enumerate(tables, start=1):
                # Clean and assign headers, handling single-column tables as well
                cleaned_df = clean_and_assign_headers(table.df)
                cleaned_df["URL"] = None

                # Skip processing if table columns still don’t match expected headers
                if len(cleaned_df.columns) < len(EXPECTED_HEADERS):
                    logging.warning(
                        f"Skipping table in {pdf_file}: insufficient columns after retry."
                    )
                    continue

                # Get matched and unmatched links for each table row
                matched_links, unmatched_links, matched_links_row_info = (
                    match_url_to_row(table, pdf_links) or ([], [], [])
                )

                # Assign URLs to matched rows
                for link_info in matched_links_row_info:
                    row_id = link_info["row_id"]
                    url = link_info["link"]
                    if row_id < len(cleaned_df):
                        cleaned_df.at[row_id, "URL"] = url

                # Match any remaining unmatched URLs
                cleaned_df, remaining_unmatched_links = match_unmatched_urls(
                    cleaned_df, unmatched_links
                )

                # Process each row in cleaned_df for scoring
                for index, row in cleaned_df.iterrows():
                    row_text = " ".join(map(str, row.values)).replace("\n", " ")
                    score, keywords, anti_keywords = (
                        self.keypaceFinder.relative_keywords_score(row_text)
                    )
                    if score > 0:
                        self._append_to_master_csv(
                            {
                                "Programme": row.get("Programme", "N/A"),
                                "Service": row.get("Service", "N/A"),
                                "Date of Transmission": row.get(
                                    "Date of Transmission", "N/A"
                                ),
                                "Issue": row.get("Issue", "N/A"),
                                "Outcome": row.get("Outcome", "N/A"),
                                "keywords": ", ".join(keywords),
                                "Score": score,
                                "type (HTML/PDF)": "PDF",
                                "url html": url_html,
                                "url Pdf": pdf_url,
                                "URL": row.get("URL", "N/A"),
                                "time": time_item,
                                "item_time": "Unknown",
                                "category": category,
                                "Page": table.page,
                                "row_index": index,
                            }
                        )
                    elif row.get("URL", False):
                        content, soup = self._scrape_web_content(row.get("URL"))
                        row2 = self.extract_information_html(soup, row.get("URL"))
                        row_text = " ".join(map(str, row2.values)).replace("\n", " ")
                        score, keywords, anti_keywords = (
                            self.keypaceFinder.relative_keywords_score(row_text)
                        )
                        score2, keywords2, anti_keywords = (
                            self.keypaceFinder.relative_keywords_score(content)
                        )
                        score = max(score,score2)
                        keywords = set(keywords+keywords2)
                        if score > 0:
                            self._append_to_master_csv(
                                {
                                    "Programme": row2.get("Programme", "N/A"),
                                    "Service": row2.get("Service", "N/A"),
                                    "Date of Transmission": row2.get(
                                        "Date of Transmission", "N/A"
                                    ),
                                    "Issue": row2.get("Issue", "N/A"),
                                    "Outcome": row2.get("Outcome", "N/A"),
                                    "keywords": ", ".join(keywords),
                                    "Score": score,
                                    "type (HTML/PDF)": "HTML",
                                    "url html": url_html,
                                    "URL": row.get("URL", "N/A"),
                                    "time": time_item,
                                    "item_time": "Unknown",
                                    "category": category,
                                    "Page": table.page,
                                    "row_index": index,
                                }
                            )

        except Exception as e:
            logging.error(f"Error processing PDF {pdf_file}: {e}")

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
                items, url_html = self.process_page(
                    page_number, base_url, category_name
                )
                if not items:
                    break
                for item in items:
                    self.process_item(item, url_html, category_name)
                page_number += 1


if __name__ == "__main__":
    scraper = BBCComplaintScraper()
    scraper.main()
