import os
import re
import requests
import time
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
import camelot
from keywords import Little_List
import pandas as pd
import fitz  # PyMuPDF

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
    local_filename = os.path.join(PDF_DIR, unquote(os.path.basename(pdf_url)))
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
def check_pdf_for_keywords(file_path, keywords):
    try:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None

        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    for keyword in keywords:
                        if keyword.lower() in text.lower():
                            logging.info(f"Keyword '{keyword}' found in: {file_path}")
                            filtered_path = move_file_to_filtered(file_path)
                            return filtered_path

        # If no keyword found, remove the file
        os.remove(file_path)
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
    return None

# Function to move file to filtered directory
def move_file_to_filtered(file_path):
    sanitized_filename = unquote(os.path.basename(file_path)).replace("%20", "_")
    filtered_path = os.path.join(FILTERED_PDF_DIR, sanitized_filename)
    os.rename(file_path, filtered_path)
    return filtered_path

# Function to process each page of the website
def process_page(page_number):
    url = BASE_URL.format(page_number)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        pdf_links = [urljoin(url, link['href']) for link in soup.find_all('a', href=True) if link['href'].endswith('.pdf')]

        # Check for a specific stopping condition
        if len(pdf_links) == 1 and 'Complaints_Framework_eng_0.pdf' in pdf_links[0]:
            logging.info("Only 'Complaints_Framework_eng_0.pdf' found on this page. Stopping the download.")
            return []

        return pdf_links
    except requests.RequestException as e:
        logging.error(f"Failed to process page {page_number}: {e}")
    return []

# Function to extract links and non-linked text from a specific page of a PDF
def extract_links_and_non_links_from_pdf(pdf_file, page=None):
    all_text = []
    document = fitz.open(pdf_file)
    page_range = range(document.page_count) if page is None else [page]

    for page_number in page_range:
        page = document[page_number]
        page_links_with_text_and_location = extract_links_with_text_and_location_from_page(page)
        all_text.extend(page_links_with_text_and_location)
        link_rects = [link_info['location'] for link_info in page_links_with_text_and_location]
        non_link_text = extract_non_link_text_from_page(page, link_rects)
        all_text.extend(non_link_text)

    document.close()
    return all_text

def extract_links_with_text_and_location_from_page(page):
    links_info = []
    links = page.get_links()

    for link in links:
        if link.get('uri'):
            link_rect = fitz.Rect(link['from'])
            text = page.get_text("text", clip=link_rect).strip()
            links_info.append({
                'text': text,
                'uri': link['uri'],
                'location': link_rect,
                "page": page.number
            })
    return links_info

def extract_non_link_text_from_page(page, link_rects):
    non_link_texts = []
    page_text = page.get_text("blocks")

    for block in page_text:
        rect = fitz.Rect(block[:4])
        if not any(rect.intersects(link_rect) for link_rect in link_rects):
            non_link_texts.append({
                'text': block[4].strip(),
                'location': rect,
                "page": page.number
            })

    return non_link_texts

# Combined function to clean strings and split them into words
def clean_and_split(strings):
    cleaned_words = []
    # Remove extra spaces and special characters
    cleaned_string = re.sub(r'[^\w\s]', '', strings).strip()
    # Split the string into words
    words = cleaned_string.split()
    # Add words to the result list
    cleaned_words.extend(words)
    return cleaned_words


def match_urls_to_table_cells(table, datas):
    """
    Match URLs to the table cells by analyzing text content in the last column.
    """
    url_rows_df = pd.DataFrame(columns=['Row Content', 'Matched URL'])
    unmatched_urls = []

    last_column_texts = table.df.iloc[:, -1].str.lower().str.strip().tolist()
    all_row_texts = table.df.apply(lambda row: " ".join(row.astype(str)).lower().strip(), axis=1).tolist()

    valid_datas = [data for data in datas if 'uri' in data and data['uri'].startswith('https://www.bbc.co.uk/contact/ecu')]
    matched_rows = {i: False for i in range(len(table.df))}

    for data in valid_datas:
        matched = False
        data_text_lower = data['text'].lower()
        print(data_text_lower)
        for i in clean_and_split(str(data_text_lower)):
            print("clean_strings->",i)

        for row_index, last_col_text in enumerate(last_column_texts):
            if matched_rows[row_index]:
                continue
            if data_text_lower in last_col_text or last_col_text in data_text_lower:
                row_text = " ".join(table.df.iloc[row_index].tolist())
                url_rows_df.loc[row_index] = [row_text, data['uri']]
                matched = True
                matched_rows[row_index] = True
                break

        if not matched:
            for row_index, all_row_text in enumerate(all_row_texts):
                print(all_row_text)
                if matched_rows[row_index]:
                    continue
                if data_text_lower in all_row_text or all_row_text in data_text_lower:
                    row_text = " ".join(table.df.iloc[row_index].tolist())
                    url_rows_df.loc[row_index] = [row_text, data['uri']]
                    matched = True
                    matched_rows[row_index] = True
                    break

        if not matched:
            unmatched_urls.append((data["text"], data["uri"]))

    for row_index, row in table.df.iterrows():
        if not matched_rows[row_index]:
            url_rows_df.loc[row_index] = [" ".join(row.astype(str).tolist()), '']

    return url_rows_df, unmatched_urls

def extract_tables_from_pdf(pdf_path):
    try:
        tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')
        if not tables or all(len(table.df) == 0 for table in tables):
            logging.info(f"No tables found with lattice flavor. Trying stream flavor for {pdf_path}.")
            tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')

        if tables:
            for i, table in enumerate(tables):
                page_num = table.parsing_report['page']
                datas = extract_links_and_non_links_from_pdf(pdf_path, page=page_num - 1)

                url_rows_df, unmatched_urls = match_urls_to_table_cells(table, datas)

                if unmatched_urls:
                    logging.warning(f"Unmatched URLs in {pdf_path} on page {page_num}: {unmatched_urls}")
                    unmatched_csv = f"{os.path.splitext(pdf_path)[0]}_unmatched_urls_page_{page_num}.csv"
                    pd.DataFrame(unmatched_urls, columns=['Text', 'URL']).to_csv(unmatched_csv, index=False)
                    logging.info(f"Saved unmatched URLs to {unmatched_csv}")

                for col in table.df.columns:
                    table.df[col] = table.df[col].str.replace(r'\n', ' ', regex=True)

                if len(url_rows_df) != len(table.df):
                    logging.warning(f"Skipping table {i} on page {page_num} in {pdf_path} due to row mismatch.")
                    continue

                df = pd.concat([table.df.reset_index(drop=True), url_rows_df['Matched URL'].reset_index(drop=True)], axis=1)
                output_csv = f"{os.path.splitext(pdf_path)[0]}_table_{i}_page_{page_num}.csv"
                df.to_csv(output_csv, index=False)
                logging.info(f"Extracted and cleaned table {i} from {pdf_path} to {output_csv}")
        else:
            logging.info(f"No tables found in {pdf_path} using both lattice and stream flavors.")
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

            future_to_url = {executor.submit(download_pdf, pdf_url): pdf_url for pdf_url in pdf_links}
            for future in as_completed(future_to_url):
                pdf_path = future.result()
                if pdf_path:
                    filtered_path = check_pdf_for_keywords(pdf_path, Little_List)
                    if filtered_path:
                        executor.submit(extract_tables_from_pdf, filtered_path)

            page_number += 1
            time.sleep(2)

if __name__ == "__main__":
    main()
