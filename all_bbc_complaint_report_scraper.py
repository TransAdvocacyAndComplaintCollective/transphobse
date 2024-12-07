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
from rapidfuzz import fuzz
from utils.keywords_finder import KeypaceFinder
import re

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

# date_pattern
# station_pattern
# programme_pattern
# Function to extract Stage 1 complaints
def extract_stage1_complaints(text):
    stage1_pattern = re.compile(
        r"(?P<Programme>.+?)\s+(?P<Service>BBC\s\w+)\s+(?P<Date>\d{2}/\d{2}/\d{4})\s+(?P<Main_Issue>.+?)\s+(?P<Number_of_Complaints>\d+)",
        # re.DOTALL
    )
    return [match.groupdict() for match in stage1_pattern.finditer(text)]

# Function to extract Stage 2 complaints
def extract_stage2_complaints(text):
    stage2_pattern = re.compile(
        r"(?P<Programme>.+?)\s+(?P<Service>BBC[\w\s]+)\s+(?P<Date>\d{2}/\d{2}/\d{4})\s+(?P<Issue>.+?)\s+(?P<Outcome>[A-z ]*)",
        # re.DOTALL
    )
    return [match.groupdict() for match in stage2_pattern.finditer(text)]


def extract_text_from_pdf(pdf_reader,pdf_path):
    """Extract text from a PDF using PyPDF2."""
    extracted_text = []
    try:
        for page in pdf_reader.pages:
            page_text = page.extract_text()
            if page_text:
                extracted_text.append(page_text)
    except Exception as e:
        print(f"Error reading {pdf_path}: {e}")
        return None
    return "\n".join(extracted_text)



date_pattern = r"((Date withheld|\d{1,2}(?:\s*(?:and|-|–|&)\s*\d{1,2})?\s*[A-Za-z]+\s*\d{4})|\d{1,2}\s(Jan|Feb|Mar|Apr|May|Jun|Mar|Apr|Jun|Jul|Sep|Oct|Nov|Dec)\w+)"
station_pattern = r"[^,]+"
programme_pattern = r"[^,]+"



def parse_programme_segments(text, pdf_path):
    """Parse text into segments based on programme patterns."""

    # Define programme patterns
    programme_pattern = r".*?"
    station_pattern = r"(BBC [^,]+|Radio [^,]+|bbc\.co\.uk)"
    date_pattern = r"\d{1,2} \w+ \d{4}"

    # Patterns dictionary with regex definitions
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
            rf"^(?P<programme>{programme_pattern}),\s*bbc\.co\.uk$"
        ),
        "station_data": re.compile(
            rf"^(?P<station>{station_pattern}),\s*(?P<date>{date_pattern})$"
        ),
        "prog_station_date_simple": re.compile(
            r"^(?P<programme>.*?),\s*(?P<station>BBC [^,]+|Radio [^,]+|bbc\.co\.uk),\s*(?P<date>\d{1,2} \w+ \d{4})?$"
        ),
        "prog_station_date_simple2": re.compile(
            r"^(?P<programme>.+?),\s*(?P<station>.+?)(?:,\s*(?P<date>\d{1,2} \w+ \d{4}))?$"
        ),
        "forgs": re.compile(
            r"^(?P<programme>[^,])*,\s(?P<station>[^,])*, (?P<date>\d{1,2}\s[A-z]*\s\d{4})$"
        ),
        "forgs2": re.compile(
            r"(?P<programme>[^,]*), (?P<station>[^,]*), (?P<date>\d{2}[^,]*\d{4})"
        ),
        "forgs3": re.compile(
            r"(?P<programme>[^,]*), (?P<station>[^,]*), (?P<date>\d{2}[^,]*)"
        ),
        "forgs4": re.compile(
            r"(?P<programme>[A-z ]*)  (?P<station>[A-z ]*)  (?P<date>\d*/\d*/\d*)"
        ),
    }
    # Corrected fallback pattern
    fallback_pattern = re.compile(
        r"^(?P<programme>[^,\n]+),\s*(?P<station>[^,\n]+),\s*(?P<date>[0-9][^,\n]*)$"
    )

    lines = text.split("\n")
    segments = []
    current_segment = None

    buffer = []
    for line in lines:
        line = line.strip()
        if not line:
            continue  # Skip empty lines

        buffer.append(line)
        buffer_text = ' '.join(buffer)

        match = None
        for pattern_name, pattern in patterns.items():
            match = pattern.match(buffer_text)
            if match:
                break

        if match:
            # Save the current segment before starting a new one
            if current_segment:
                segments.append(current_segment)

            # Start a new segment
            current_segment = {
                "Programme": match.group("programme").strip() if "programme" in match.groupdict() and match.group("programme") else None,
                "Station": match.group("station").strip() if "station" in match.groupdict() and match.group("station") else None,
                "Date": match.group("date").strip() if "date" in match.groupdict() and match.group("date") else None,
                "Text Between": "",
                "PDF Path": pdf_path,
                "Page": "N/A",  # Placeholder; update if page info is available
            }
            buffer = []  # Reset buffer
        elif fallback_pattern.match(buffer_text):
            # Fallback parsing
            if current_segment:
                segments.append(current_segment)

            fallback_match = fallback_pattern.match(buffer_text)
            current_segment = {
                "Programme": fallback_match.group("programme").strip() if "programme" in fallback_match.groupdict() and fallback_match.group("programme") else None,
                "Station": fallback_match.group("station").strip() if "station" in fallback_match.groupdict() and fallback_match.group("station") else None,
                "Date": fallback_match.group("date").strip() if "date" in fallback_match.groupdict() and fallback_match.group("date") else None,
                "Text Between": "",
                "PDF Path": pdf_path,
                "Page": "N/A",  # Placeholder; update if page info is available
            }
            buffer = []  # Reset buffer
        else:
            # Keep accumulating lines
            continue

    # Append the last segment if it exists
    if current_segment:
        segments.append(current_segment)

    return segments

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
    SKIP_URLS = [
        "https://www.ofcom.org.uk/tv-radio-and-on-demand/broadcast-codes/broadcast-code",
        "http://www.bbc.co.uk/complaints/complaint/",
        "http://www.bbc.co.uk/complaints/handle-complaint/",
    ]
    if url in SKIP_URLS:
        return ""
    try:
        response = requests.get(url, timeout=60*10)
        if response.status_code != 200:
            return ""
        response.raise_for_status()  # Check if the request was successful
        soup = BeautifulSoup(response.content, "lxml")
        return " ".join([p.text for p in soup.find_all("p")])
    except requests.RequestException as e:
        logging.error(f"Failed to scrape {url}: {e}")
        return ""


# Function to clean and assign headers to a table
def clean_and_assign_headers(df):
    """
    Cleans the dataframe by removing newline characters and assigns headers based on EXPECTED_HEADERS.

    Args:
        df (pd.DataFrame): The input dataframe to be cleaned and assigned headers.

    Returns:
        pd.DataFrame: Cleaned dataframe with assigned headers, or None if the column count doesn't match.
    """
    # Define the expected headers
    EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Number of Complaints"]
    print("count rows",len(df))
    # Check if the number of columns matches the expected headers
    if len(df.columns) != len(EXPECTED_HEADERS):
        print(f"Column count mismatch: Expected {len(EXPECTED_HEADERS)}, but got {len(df.columns)}.")
        return None

    # Assign the expected headers to the DataFrame
    df.columns = EXPECTED_HEADERS

    # Function to clean newline characters from the data
    def clean_newlines(series):
        return series.apply(lambda x: x.replace("\n", " ") if isinstance(x, str) else x)

    # Apply the newline cleaning function to each column
    df = df.apply(clean_newlines)

    return df


# Function to try matching unmatched URLs
def match_unmatched_urls(cleaned_df, unmatched_links):
    """
    Matches unmatched URLs to the most appropriate row in the DataFrame using fuzzy matching.

    Args:
        cleaned_df (pd.DataFrame): DataFrame containing complaint data.
        unmatched_links (list): List of URLs that need to be matched.

    Returns:
        pd.DataFrame: Updated DataFrame with matched URLs.
        list: List of URLs that couldn't be matched.
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Function to fetch and process web content
    def fetch_and_process(url):
        try:
            web_content = scrape_web_content(url).lower()
            return url, web_content
        except requests.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return url, ""
    # Fetch web content concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_and_process, url): url for url in unmatched_links}
        for future in as_completed(future_to_url):
            url, web_content = future.result()
            if not web_content:
                continue

            # Prepare a list of potential matches
            potential_matches = []
            for index, row in cleaned_df.iterrows():
                row_text = " ".join(map(str, row.dropna().values)).lower()
                match_score = fuzz.token_set_ratio(web_content, row_text)
                potential_matches.append((index, match_score))

            # Find the best match above a certain threshold
            best_match = max(potential_matches, key=lambda x: x[1], default=(None, 0))
            best_match_index, best_match_score = best_match

            if best_match_score > 70:  # Threshold can be adjusted
                if pd.isna(cleaned_df.at[best_match_index, "URL"]):
                    cleaned_df.at[best_match_index, "URL"] = url
                    unmatched_links.remove(url)
                    logging.info(f"Matched URL {url} to row {best_match_index} with score {best_match_score}")
            else:
                logging.info(f"No suitable match found for URL {url}")

    return cleaned_df, unmatched_links


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


def extract_info(pdf_path: str) -> List[Dict[str, Any]]:
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

    def process_page(self, page_number: int, base_url: str, category_name: str) -> Tuple[List[Dict[str, Any]], str]:
        url = base_url.format(page_number)
        if page_number > 100:
            return [], None
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 404:
                return [], None
            response.raise_for_status()
            time.sleep(2)
            soup = BeautifulSoup(response.content, "lxml", from_encoding="utf-8")
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
            logging.error(f"Failed to process page {page_number}, {base_url}: {e}")
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
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
            }
            response = requests.get(full_url, timeout=60, headers=headers)
            if response.status_code != 200:
                logging.warning(f"Non-200 status code received: {response.status_code}")
                return "", None
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml")

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
        programme = soup.select_one("#block-bbc-contact-page-title")
        print("extract_information_html -> ",programme)
        print(url)
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
        print(item)
        pdf_url = item.get("pdf_link")
        if pdf_url and "pdf" in pdf_url:
            pdf_file = self._download_pdf(pdf_url)
            if pdf_file:
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
        good =False
        if content:
            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(
                content
            )
            if score > 0 :
                good =True
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
        if not good:
            pass
            
    def _deduplicate_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicates the list of extracted rows based on key fields.

        Args:
            rows (List[Dict[str, Any]]): List of extracted rows.

        Returns:
            List[Dict[str, Any]]: Deduplicated list of rows.
        """
        df = pd.DataFrame(rows)

        # Define the subset of columns to identify duplicates
        subset = ["Programme", "Service", "Date of Transmission", "Issue", "Page"]

        # Add a condition to handle "PDF (Backup)" rows differently
        for index, row in df.iterrows():
            if row["type (HTML/PDF)"] == "PDF (Backup)":
                # Ensure that rows with "PDF (Backup)" are uniquely identified by `Page`
                # If "Programme", "Service", etc., are missing, use only `Page` for deduplication
                row["Programme"] = row["Programme"] or f"Page {row['Page']}"
                row["Service"] = row["Service"] or f"Page {row['Page']}"
                row["Date of Transmission"] = row["Date of Transmission"] or f"Page {row['Page']}"
                row["Issue"] = row["Issue"] or f"Page {row['Page']}"

        # Drop duplicates based on the updated subset
        df_unique = df.drop_duplicates(subset=subset, keep='first')

        # Reset the index for consistency
        df_unique.reset_index(drop=True, inplace=True)

        return df_unique.to_dict(orient='records')


    def extract_tables_from_pdf(
        self, pdf_file: str, url_html: str, pdf_url: str, time_item: str, category: str
    ) -> None:
        try:
            reader = PdfReader(pdf_file)
            print("_extract_links_from_pdf",pdf_file)
            pdf_links = self._extract_links_from_pdf(reader)#
            print("_extract_full_text",pdf_file)
            all_text = self._extract_full_text(reader)#
            print("_parse_text_segments",pdf_file)
            segments = self._parse_text_segments(all_text, pdf_file)

            # Initialize a list to collect all extracted rows
            all_extracted_rows = []

            # 1. Process text segments
            print("_process_text_segments",pdf_file)
            extracted_from_segments = self._process_text_segments(
                segments, pdf_file, url_html, pdf_url, time_item, category
            )
            # all_extracted_rows.extend(extracted_from_segments)

            # 2. Extract tables using Camelot
            print("_extract_tables_with_camelot",pdf_file)
            tables = self._extract_tables_with_camelot(pdf_file)
            extracted_from_tables = self._process_extracted_tables(
                tables, pdf_links, url_html, pdf_url, time_item, category
            )
            all_extracted_rows.extend(extracted_from_tables)

            # 3. Fallback regex extraction
            print("_fallback_regex_extraction",pdf_file)
            extracted_from_fallback = self._fallback_regex_extraction(
                reader, all_text, url_html, pdf_url, time_item, category
            )
            all_extracted_rows.extend(extracted_from_fallback)
            if len(all_extracted_rows) == 0:
                # 4. Backup keyword scoring
                extracted_from_backup = self._backup_keyword_scoring(
                    all_text, reader, url_html, pdf_url, time_item, category
                )
                all_extracted_rows.extend(extracted_from_backup)

            # 5. Deduplicate extracted rows
            unique_extracted_rows = self._deduplicate_rows(all_extracted_rows)

            # 6. Append unique rows to the master CSV
            for row in unique_extracted_rows:
                self._append_to_master_csv(row)

        except Exception as e:
            logging.error(f"Error processing PDF {pdf_file}: {e}")


    def _extract_full_text(self, reader: PdfReader) -> str:
        """Extracts all text from the PDF."""
        all_text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                all_text += page_text
        return all_text

    def _parse_text_segments(self, text: str, pdf_path: str) -> List[Dict[str, Any]]:
        """Parses text into structured segments based on predefined patterns."""
        segments = parse_programme_segments(text, pdf_path)
        return segments

    def _process_text_segments(
        self,
        segments: List[Dict[str, Any]],
        pdf_file: str,
        url_html: str,
        pdf_url: str,
        time_item: str,
        category: str
    ) -> List[Dict[str, Any]]:
        extracted_rows = []
        print(segments)
        for segment in segments:
            print(segment)
            if segment["Text Between"] is None:
                continue
            score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(segment["Text Between"])
            if score > 0:
                row = {
                    "Programme": segment.get("Programme", "N/A"),
                    "Service": segment.get("Service", "N/A"),
                    "Date of Transmission": segment.get("Date", "N/A"),
                    "Issue": segment.get("Text Between", "N/A"),
                    "Outcome": "N/A",
                    "keywords": ", ".join(keywords),
                    "Score": score,
                    "type (HTML/PDF)": "PDF_non_table",
                    "url html": url_html,
                    "url Pdf": pdf_url,
                    "URL": "",
                    "time": time_item,
                    "item_time": "Unknown",
                    "category": category,
                    "Page": segment.get("Page", "N/A").replace("\n", " "),
                    "row_index": "N/A",
                }
                extracted_rows.append(row)
        return extracted_rows


    def _extract_tables_with_camelot(self, pdf_file: str) -> List[camelot.core.Table]:
        """Attempts to extract tables from the PDF using Camelot."""
        try:
            tables = camelot.read_pdf(
                pdf_file,
                pages="all",
                flavor="lattice",
                strip_text="\n",
            )
            if not tables:
                tables = camelot.read_pdf(pdf_file, pages="all", flavor="stream")
            return tables
        except Exception as e:
            logging.warning(f"Failed to read tables from {pdf_file} using Camelot: {e}")
            return []

    def _process_extracted_tables(
        self,
        tables: List[camelot.core.Table],
        pdf_links: Dict[int, List[Dict[str, Any]]],
        url_html: str,
        pdf_url: str,
        time_item: str,
        category: str
    ) -> List[Dict[str, Any]]:
        extracted_rows = []
        for table in tables:
            cleaned_df = clean_and_assign_headers(table.df)
            if cleaned_df is None:
                continue

            cleaned_df["URL"] = None  # Placeholder for URLs

            matched_links, unmatched_links, matched_links_row_info = match_url_to_row(table, pdf_links)

            # Assign URLs to DataFrame rows
            for link_info in matched_links_row_info:
                row_id = link_info["row_id"]
                url = link_info["link"]
                if row_id < len(cleaned_df):
                    cleaned_df.at[row_id, "URL"] = url

            # Process each row in the DataFrame
            for index, row in cleaned_df.iterrows():
                row_text = " ".join(map(str, row.values)).replace("\n", " ")
                score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(row_text)
                if score > 0:
                    extracted_row = {
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
                        "URL": row.get("URL", "N/A"),
                        "time": time_item,
                        "item_time": "Unknown",
                        "category": category,
                        "Page": table.page,
                        "row_index": index,
                    }
                    extracted_rows.append(extracted_row)
        return extracted_rows

    def _fallback_regex_extraction(
        self,
        reader: PdfReader,
        all_text: str,
        url_html: str,
        pdf_url: str,
        time_item: str,
        category: str
    ) -> List[Dict[str, Any]]:
        extracted_rows = []
        for page_num, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if not page_text:
                continue

            matches = re.findall(
                r"^(.*?),\s*(BBC [\w\s]+),\s*(\d{1,2}/\d{1,2}/\d{4}),\s*(.*?)\s*(Upheld|Not upheld|Resolved|Closed)$",
                page_text,
                re.MULTILINE,
            )

            for match in matches:
                programme, service, date, issue, outcome = match
                row_text = " ".join(match).replace("\n", " ")
                score, keywords, anti_keywords = self.keypaceFinder.relative_keywords_score(row_text)
                if score > 0:
                    row = {
                        "Programme": programme.strip(),
                        "Service": service.strip(),
                        "Date of Transmission": date.strip(),
                        "Issue": issue.strip(),
                        "Outcome": outcome.strip(),
                        "keywords": ", ".join(keywords),
                        "Score": score,
                        "type (HTML/PDF)": "PDF (Fallback)",
                        "url html": url_html,
                        "url Pdf": pdf_url,
                        "URL": "",
                        "time": time_item,
                        "item_time": "Unknown",
                        "category": category,
                        "Page": page_num + 1,
                        "row_index": "N/A",
                    }
                    extracted_rows.append(row)
        return extracted_rows

    def _backup_keyword_scoring(
        self,
        all_text: str,
        reader: PdfReader,
        url_html: str,
        pdf_url: str,
        time_item: str,
        category: str
    ) -> List[Dict[str, Any]]:
        extracted_rows = []
        for page_num, page in enumerate(reader.pages):
            text_page = page.extract_text()
            score_backup, keywords_backup, anti_keywords_backup = self.keypaceFinder.relative_keywords_score(text_page)
            if score_backup > 0:
                row = {
                    "Programme": "N/A",
                    "Service": "N/A",
                    "Date of Transmission": "N/A",
                    "Issue": "N/A",
                    "Outcome": "N/A",
                    "keywords": ", ".join(keywords_backup),
                    "Score": score_backup,
                    "type (HTML/PDF)": "PDF (Backup)",
                    "url html": url_html,
                    "url Pdf": pdf_url,
                    "URL": "",
                    "time": time_item,
                    "item_time": "Unknown",
                    "category": category,
                    "Page": page_num + 1,
                    "row_index": "N/A",
                }
                extracted_rows.append(row)
        return extracted_rows

    def _append_to_master_csv(self, row_data: Dict[str, Any]) -> None:
        """Appends a row of data to the master CSV with thread safety."""
        try:
            # Reorder the row data to match the master headers
            ordered_row = {key: row_data.get(key, "N/A") for key in master_headers}

            # Write the row to the CSV with a lock to prevent concurrent write issues
            with self.csv_lock:
                pd.DataFrame([ordered_row]).to_csv(
                    self.MASTER_CSV_OUTPUT, mode="a", header=False, index=False
                )

        except Exception as e:
            logging.error(f"Failed to append row to master CSV: {e}")



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
                print(items, url_html)
                if not items:
                    break
                for item in items:
                    self.process_item(item, url_html, category_name)
                page_number += 1


if __name__ == "__main__":
    scraper = BBCComplaintScraper()
    scraper.main()
