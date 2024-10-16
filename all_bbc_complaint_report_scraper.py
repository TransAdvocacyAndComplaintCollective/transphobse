import difflib
import threading
from urllib.parse import urlparse, urljoin, unquote
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from io import BytesIO
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import camelot
from utils.keywords_finder import relative_keywords_score
import re

# Directories and configuration
PDF_DIR = "data/bbc_complaint_pdfs"
CSV_OUTPUT_DIR = "data/data/csv_outputs"
MASTER_CSV_OUTPUT = (
    "data/bbc_transphobia_complaint_service_reports_keywords_matched.csv"
)
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

EXPECTED_HEADERS = ["Programme", "Service", "Date of Transmission", "Issue", "Outcome"]

# Thread lock for writing to the CSV
csv_lock = threading.Lock()

# Create the master CSV if it doesn't exist
if not os.path.exists(MASTER_CSV_OUTPUT):
    master_csv_headers = [
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
    with open(MASTER_CSV_OUTPUT, "w") as f:
        pd.DataFrame(columns=master_csv_headers).to_csv(f, index=False)


def contains_keywords(text, bypass_anti=True):
    logging.info(f"Scanning text for keywords...")
    final_score, keywords, anti_keywords = relative_keywords_score(text, bypass_anti)
    logging.info(
        f"Keywords found: {keywords}, Anti-keywords found: {anti_keywords}, Final Score: {final_score}"
    )
    return final_score > 0, keywords, anti_keywords, final_score


def is_valid_bbc_url(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc.endswith("bbc.co.uk") or parsed_url.netloc.endswith("bbc.com") or parsed_url.netloc.endswith("bbci.co.uk")


def get_full_url(base_url, relative_url):
    if not relative_url:
        return None
    if relative_url.startswith("http"):
        full_url = relative_url
    else:
        full_url = urljoin(base_url, relative_url)
    if is_valid_bbc_url(full_url):
        return full_url
    else:
        logging.info(f"Skipping non-BBC URL: {full_url}")
        return None


def scrape_web_content(full_url):
    if not full_url:
        logging.error(f"Invalid URL: {full_url}")
        return "", None

    try:
        logging.info(f"Scraping web content from URL: {full_url}")
        response = requests.get(full_url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        return " ".join([p.text for p in soup.find_all("p")]), soup
    except requests.RequestException as e:
        logging.error(f"Failed to scrape {full_url}: {e}")
        return "", None


def is_bbox_overlap(bbox1, bbox2):
    x1_1, y1_1, x2_1, y2_1 = bbox1
    x1_2, y1_2, x2_2, y2_2 = bbox2
    return not (x2_1 < x1_2 or x2_2 < x1_1 or y2_1 < y1_2 or y2_2 < y1_1)


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
                if link and isinstance(link, dict) and "rect" in link and "uri" in link:
                    link_bbox = (
                        link["rect"][0],
                        link["rect"][1],
                        link["rect"][2],
                        link["rect"][3],
                    )
                    if is_bbox_overlap(cell_bbox, link_bbox):
                        matched_links.append((cell_bbox, link["uri"]))
                        print(
                            f"Matched link {link['uri']} with cell bounding box {cell_bbox}"
                        )
                        output.append(
                            {
                                "row_id": row_id,
                                "cell_bbox": cell_bbox,
                                "link": link["uri"],
                            }
                        )
                        matched = True
                        break
                if not matched and link and "uri" in link:
                    unmatched_links.add(link["uri"])

    return matched_links, list(unmatched_links), output


def extract_links_from_pdf(reader):
    output = {}
    try:
        for page_num, page in enumerate(reader.pages):
            output[page_num] = []
            if "/Annots" in page:
                annotations = page["/Annots"]
                for annotation in annotations:
                    annotation_obj = annotation.get_object()
                    if annotation_obj and isinstance(annotation_obj, dict):
                        if "/A" in annotation_obj and "/URI" in annotation_obj["/A"]:
                            uri = annotation_obj["/A"]["/URI"]
                            rect = annotation_obj.get("/Rect", None)
                            if rect:
                                output[page_num].append(
                                    {"page": page_num + 1, "uri": uri, "rect": rect}
                                )
                            else:
                                output[page_num].append(
                                    {"page": page_num + 1, "uri": uri, "rect": None}
                                )
    except Exception as e:
        logging.error(f"Failed to extract links from PDF: {e}")
    return output


def extract_complaints_from_pdf(reader):
    regex = r"^(.*?),\s*(.*?),?\s*(\d{1,2}\s)?(January|February|March|April|May|June|July|August|September|October|November|December)?\s?(19\d{2}|20\d{2})[ ]*$"
    full_text = ""
    page_number_tracker = []

    for page_num, page in enumerate(reader.pages, start=1):
        text = page.extract_text()
        if text:
            start_pos = len(full_text)
            full_text += text
            end_pos = len(full_text)
            page_number_tracker.append((page_num, start_pos, end_pos))
        else:
            print(f"Warning: Could not extract text from page {page_num}.")
            continue

    matches = list(re.finditer(regex, full_text, re.MULTILINE))

    if not matches:
        print("No matches found.")
        return []

    previous_end = 0

    def get_page_for_position(pos):
        for page_num, start_pos, end_pos in page_number_tracker:
            if start_pos <= pos < end_pos:
                return page_num
        return None

    outcome_regex = r"(Ruling|Upheld|Partly upheld|Resolved)"

    for i, match in enumerate(matches):
        start_pos = match.start()
        end_pos = match.end()
        page_num = get_page_for_position(start_pos)

        title = match.group(0).strip()

        if previous_end != 0:
            text_between = full_text[previous_end:start_pos].strip()
        else:
            text_between = full_text[:start_pos].strip()

        found, keywords, anti_keywords, score = contains_keywords(text_between)

        outcome_match = re.search(outcome_regex, text_between, re.IGNORECASE)
        outcome = outcome_match.group(0) if outcome_match else "No outcome found"
        if score > 0:
            yield {
                "Programme": match.group(1),
                "Service": match.group(2),
                "Date of Transmission": (
                    (match.group(4) + " " + match.group(5))
                    if match.group(4) and match.group(5)
                    else match.group(5)
                ),
                "Details": text_between,
                "Page": page_num,
                "Found": found,
                "Keywords": keywords,
                "Anti_keywords": anti_keywords,
                "Score": score,
                "Outcome": outcome,
            }

        previous_end = end_pos

    remaining_text = full_text[previous_end:].strip()
    if remaining_text:
        last_page_num = get_page_for_position(previous_end)
        found, keywords, anti_keywords, score = contains_keywords(remaining_text)
        outcome_match = re.search(outcome_regex, remaining_text, re.IGNORECASE)
        outcome = outcome_match.group(0) if outcome_match else "No outcome found"
        if score > 0:
            yield {
                "Programme": match.group(1),
                "Service": match.group(2),
                "Date of Transmission": (
                    (match.group(4) + " " + match.group(5))
                    if match.group(4) and match.group(5)
                    else match.group(5)
                ),
                "Details": remaining_text,
                "Page": last_page_num,
                "Found": found,
                "Keywords": keywords,
                "Anti_keywords": anti_keywords,
                "Score": score,
                "Outcome": outcome,
            }


def match_unmatched_urls(cleaned_df, unmatched_links):
    unmatched_links_temp = unmatched_links.copy()
    for url in unmatched_links:
        best_match_row = None
        best_match_score = 0
        url_text = url.lower()

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

            if match_score > best_match_score and match_score > 0.5:
                best_match_row = index
                best_match_score = match_score

        if best_match_row is None:
            web_content, soup = scrape_web_content(url)
            for index, row in cleaned_df.iterrows():
                row_text = " ".join(map(str, row)).lower()
                match_score = difflib.SequenceMatcher(
                    None, web_content, row_text
                ).ratio()

                if match_score > best_match_score and match_score > 0.5:
                    best_match_row = index
                    best_match_score = match_score

        if best_match_row is not None and pd.isna(cleaned_df.at[best_match_row, "URL"]):
            cleaned_df.at[best_match_row, "URL"] = url
            unmatched_links_temp.remove(url)

    return cleaned_df, unmatched_links_temp


def extract_tables_from_pdf(pdf_path, url_html, pdf_url, time_item, category):
    try:
        logging.info(f"Extracting tables from PDF: {pdf_path}")

        reader = PdfReader(pdf_path)

        output_pdf_links = extract_links_from_pdf(reader)

        tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")

        if not tables or all(len(table.df) == 0 for table in tables):
            logging.info(
                f"No valid tables found in {pdf_path}. Treating as 'No Table Found'."
            )
            complaints = list(extract_complaints_from_pdf(reader))
            for complaint in complaints:
                if complaint["Score"] > 0:
                    append_to_master_csv(
                        complaint,
                        complaint["Keywords"],
                        complaint["Anti_keywords"],
                        complaint["Score"],
                        "PDF (No Table Found Headings)",
                        url_html,
                        pdf_path,
                        pdf_url,
                        time_item,
                        category,
                        page_No=complaint.get("Page"),
                    )
            if not complaints:
                handle_no_table(
                    pdf_url,
                    pdf_path,
                    url_html,
                    time_item,
                    category,
                )
            return

        for i, table in enumerate(tables):
            if not table.df.empty:
                cleaned_df = clean_and_assign_headers(table.df)

                matched_links, unmatched_links, matched_links_row_info = (
                    match_url_to_row(table, output_pdf_links)
                )

                cleaned_df["URL"] = None
                for link_info in matched_links_row_info:
                    row_id = link_info["row_id"]
                    url = link_info["link"]
                    if row_id < len(cleaned_df):
                        cleaned_df.at[row_id, "URL"] = url

                cleaned_df, remaining_unmatched_links = match_unmatched_urls(
                    cleaned_df, unmatched_links
                )

                csv_file_path = os.path.join(
                    CSV_OUTPUT_DIR, f"{os.path.basename(pdf_path)}_table_{i+1}.csv"
                )
                cleaned_df.to_csv(csv_file_path, index=False)
                logging.info(f"Saved and cleaned table {i+1} to {csv_file_path}")

                for index, row in cleaned_df.iterrows():
                    full_text = " ".join(map(str, row.values))
                    found, keywords, anti_keywords, score = contains_keywords(full_text)

                    if row.get("URL") and found:
                        url = row.get("URL")
                        req = requests.get(url)
                        if req.status_code == 200:
                            text = req.text
                            html = BeautifulSoup(text, "html.parser")
                            body = " ".join([p.text for p in html.find_all("p")])
                            found, keywords, anti_keywords, score = contains_keywords(body)
                            try:
                                item_time = html.find("time").text
                            except:
                                item_time = "Unknown Time"
                    else:
                        item_time = "Unknown Time"
                    if found:
                        append_to_master_csv(
                            row,
                            keywords,
                            anti_keywords,
                            score,
                            "PDF_row",
                            url_html,
                            pdf_path,
                            pdf_url,
                            time_item,
                            category,
                            item_time=item_time,
                            page_No=i + 1,
                            row_index=index,
                        )

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        logging.error(f"Failed to extract tables from {pdf_path}: {e}\nTraceback: {tb}")
        append_to_master_csv(
            {},
            [],  
            [],  
            0,  
            "PDF_ERROR",
            url_html,
            pdf_path,
            pdf_url,
            time_item,
            category,
        )


def download_pdf_or_treat_as_pdf(item, url_html, retries=3):
    pdf_url = item.get("pdf_link", "")
    if not is_valid_bbc_url(pdf_url):
        logging.error(f"Skipping non-BBC URL: {pdf_url}")
        return None, None, None, None, None
    if "pdf" in pdf_url:
        local_filename = os.path.join(PDF_DIR, unquote(os.path.basename(pdf_url)))
        logging.info(f"Attempting to download PDF: {pdf_url}")
        for attempt in range(retries):
            try:
                response = requests.get(pdf_url, timeout=10)
                response.raise_for_status()
                with open(local_filename, "wb") as f:
                    f.write(response.content)
                return local_filename, url_html, pdf_url, item.get("time"), None
            except requests.RequestException as e:
                logging.error(
                    f"Failed to download {pdf_url}: {e}. Retrying {attempt + 1}/{retries}..."
                )
                time.sleep(2)
        return None, None, None, None, None
    else:
        html_content, soup = scrape_web_content(pdf_url)
        return None, url_html, pdf_url, item.get("time"), html_content


def clean_pdf_text(text):
    text = " ".join(text.split())
    return text


def extract_text_from_pdf(pdf_path):
    try:
        logging.info(f"Extracting text from PDF: {pdf_path}")
        reader = PdfReader(pdf_path)
        extracted_text = []
        for page in reader.pages:
            try:
                extracted_text.append(page.extract_text())
            except Exception as page_error:
                logging.error(f"Failed to extract text from page: {page_error}")
        text = " ".join(extracted_text)
        return clean_pdf_text(text)
    except Exception as e:
        logging.error(f"Failed to extract text from PDF {pdf_path}: {e}")
        return ""


def clean_and_assign_headers(df):
    logging.info(f"Cleaning and assigning headers to table")
    df = df.apply(
        lambda col: col.map(lambda x: x.replace("\n", " ") if isinstance(x, str) else x)
    )
    if len(df.columns) != len(EXPECTED_HEADERS):
        df.columns = ["Column" + str(i) for i in range(len(df.columns))]
    else:
        df.columns = EXPECTED_HEADERS
    return df


def append_to_master_csv(
    row,
    keywords,
    anti_keywords,
    score,
    content_type,
    url_html,
    pdf_local_file,
    pdf_url,
    time_item,
    category,
    item_time=None,
    page_No=None,
    row_index=None,
):
    data = {
        "Programme": row.get("Programme", "N/A"),
        "Service": row.get("Service", "N/A"),
        "Date of Transmission": row.get("Date of Transmission", "N/A"),
        "Issue": row.get("Issue", "N/A"),
        "Outcome": row.get("Outcome", "N/A"),
        "keywords": ", ".join(keywords) if keywords else "N/A",
        "anti-keywords": ", ".join(anti_keywords) if anti_keywords else "N/A",
        "Score": score,
        "type (HTML/PDF)": content_type,
        "url html": url_html if url_html else "N/A",
        "url Pdf": pdf_url if pdf_url else "N/A",
        "URL": row.get("URL", "N/A"),
        "time": time_item,
        "category": category,
        "item_time": item_time,
        "Page": row.get("Page", page_No),
        "row_index": row_index,
    }
    with csv_lock:
        header_needed = not os.path.exists(MASTER_CSV_OUTPUT)
        pd.DataFrame([data]).to_csv(
            MASTER_CSV_OUTPUT, mode="a", header=header_needed, index=False
        )


def handle_no_table(pdf_url, pdf_local_file, url_html, time_item, category):
    logging.info(f"Handling no table case for PDF: {pdf_local_file}")
    pdf_text = extract_text_from_pdf(pdf_local_file)
    logging.info(
        f"Extracted PDF text: {pdf_text[:200]}..."
    )
    if pdf_text:
        found, keywords, anti_keywords, score = contains_keywords(pdf_text)
        logging.info(
            f"Keyword scan result: Found={found}, Score={score}, Keywords={keywords}"
        )
        if found or len(keywords) > 0:
            append_to_master_csv(
                {},
                keywords,
                anti_keywords,
                score,
                "PDF (No Table)",
                url_html,
                pdf_local_file,
                pdf_url,
                time_item,
                category,
            )
    else:
        logging.warning("No text found in PDF")
        append_to_master_csv(
            {},
            [],
            [],
            0,
            "No Text Found (PDF)",
            url_html,
            pdf_local_file,
            pdf_url,
            time_item,
            category,
        )


def process_page(page_number, base_url, category_name):
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
                full_link = get_full_url(url, link.get("href"))
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


def extract_information_html(soup, full_url):
    title = soup.select_one("#block-bbc-contact-page-title")
    row = {
        "Programme": "N/A",
        "Service": "N/A",
        "Date of Transmission": "N/A",
        "Issue": "N/A",
        "Outcome": "N/A",
        "URL": full_url,
    }

    if title:
        title_text = title.get_text().replace("\n", "").strip()
        split_title = title_text.split(",")
        if len(split_title) == 3:
            row["Programme"] = split_title[0].strip()
            row["Service"] = split_title[1].strip()
            row["Date of Transmission"] = split_title[2].strip()
        elif len(split_title) == 2:
            row["Programme"] = split_title[0].strip()
            row["Service"] = split_title[1].strip()
        else:
            row["Programme"] = title_text

    issue_status = soup.select_one(".ecu-complaint > div > p")
    if issue_status:
        row["Issue"] = issue_status.get_text().strip()

    outcome_status = soup.select_one(".ecu-outcome > div > p")
    if outcome_status:
        row["Outcome"] = outcome_status.get_text().strip()

    return row


def main():
    categories = {
        "recent-ecu": "https://www.bbc.co.uk/contact/recent-ecu?page={}",
        "archived-ecu": "https://www.bbc.co.uk/contact/archived-ecu?page={}",
        "recent-complaints": "https://www.bbc.co.uk/contact/complaints/recent-complaints?page={}",
        "complaint-service-reports": "https://www.bbc.co.uk/contact/complaint-service-reports?page={}",
    }

    for category_name, base_url in categories.items():
        seen = set()
        page_number = 0
        while True:
            items, url_html = process_page(page_number, base_url, category_name)
            if not url_html:
                break
            if not items or len(items) == 0:
                break
            if url_html in seen:
                page_number += 1
                continue
            seen.add(url_html)

            for item in items:
                if item.get("pdf_link"):
                    pdf_local_file, url_html, pdf_url, time_item, html_content = (
                        download_pdf_or_treat_as_pdf(item, url_html)
                    )
                    if pdf_local_file:
                        extract_tables_from_pdf(
                            pdf_local_file,
                            url_html,
                            pdf_url,
                            time_item,
                            category_name,
                        )
                    elif html_content:
                        found, keywords, anti_keywords, score = contains_keywords(
                            html_content
                        )
                        row = {
                            "Programme": "N/A",
                            "Service": "N/A",
                            "Date of Transmission": "N/A",
                            "Issue": "N/A",
                            "Outcome": "N/A",
                            "URL": item.get("pdf_link"),
                        }

                        if found:
                            append_to_master_csv(
                                row,
                                keywords,
                                anti_keywords,
                                score,
                                "HTML",
                                url_html,
                                "",
                                pdf_url,
                                time_item,
                                category_name,
                            )
                else:
                    full_url = get_full_url(url_html, item.get("link"))
                    html_content, soup = scrape_web_content(full_url)
                    if not soup:
                        continue
                    row = extract_information_html(soup, full_url)

                    if html_content:
                        found, keywords, anti_keywords, score = contains_keywords(
                            html_content
                        )
                        if found:
                            append_to_master_csv(
                                row,
                                keywords,
                                anti_keywords,
                                score,
                                "HTML",
                                url_html,
                                "",
                                "",
                                item.get("time"),
                                category_name,
                            )

            page_number += 1


if __name__ == "__main__":
    main()
