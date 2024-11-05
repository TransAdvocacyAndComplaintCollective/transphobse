from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from io import BytesIO
import csv
import urllib.parse
from utils.keywords_finder import KeypaceFinder

keypaceFinder =  KeypaceFinder()
# Function to scan text for keywords
def contains_keywords(text, bypass_anit=False):
    final_score, keywords,_ = keypaceFinder.relative_keywords_score(text)
    if final_score > 0:
        return True, keywords
    return False, []

# Function to download and process the PDF from the complaint page
def pass_complaint_page(url, csv_writer):
    try:
        data = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        data.raise_for_status()
        html = BeautifulSoup(data.text, "html.parser")

        # Scan for keywords in the HTML content first
        html_found, html_keywords = contains_keywords(html.get_text())
        if html_found:
            # Use find instead of find_all to avoid ResultSet issues
            header_element = html.select_one("#block-bbc-contact-page-title")
            time_element = html.find("time")
            time = time_element.text if time_element else "Unknown Time"
            header = header_element.text if header_element else "Unknown Header"
            csv_writer.writerow([url, "HTML", header, time, ";".join(html_keywords)])
            print(f"Keywords found in HTML content of page: {url} at {time} with header '{header}'")

        # Look for the PDF download link
        for i in html.find_all("a"):
            if "href" in i.attrs and "Download PDF File " in i.text:
                pdf_url = i["href"]

                # If the PDF URL is relative, prepend the base URL
                if not pdf_url.startswith("http"):
                    pdf_url = urllib.parse.urljoin("https://www.bbc.co.uk" ,pdf_url)

                # Download the PDF
                pdf_data = requests.get(pdf_url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
                pdf_data.raise_for_status()  # Check for request errors

                # Read the PDF content
                reader = PdfReader(BytesIO(pdf_data.content))

                # Extract text from all pages and scan for keywords
                pdf_text = ""
                for page in reader.pages:
                    pdf_text += page.extract_text()

                pdf_found, pdf_keywords = contains_keywords(pdf_text, bypass_anit=True)
                if pdf_found:
                    time_element = html.find("time")
                    time = time_element.text if time_element else "Unknown Time"
                    header_element = html.find("h1", class_="gel-trafalgar")
                    header = header_element.text if header_element else "Unknown Header"
                    csv_writer.writerow([pdf_url, "PDF", header, time, ";".join(pdf_keywords)])
                    print(f"Keywords found in PDF: {pdf_url} at {time} with header '{header}'")
                return

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch page or PDF: {url} -> {e}")
    except Exception as e:
        print(f"Error reading PDF from {url}: {e}")

# Main function to iterate through complaint pages
def main():
    count = 0

    # Open the CSV file to log matches
    with open("data/recent_complaints_keyword_matches.csv", mode="w", newline="", encoding="utf-8") as file:
        csv_writer = csv.writer(file)
        # Updated header to include the 'Header' column
        csv_writer.writerow(["URL", "Source", "Header", "Time", "Keywords"])

        while True:
            url = f"https://www.bbc.co.uk/contact/complaints/recent-complaints?page={count}&category=All"

            try:
                data = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                data.raise_for_status()  # Check if the request was successful
                html = BeautifulSoup(data.text, "html.parser")

                url_count = 0
                for i in html.find_all("a"):
                    if "href" in i.attrs and ".pdf" in i["href"]:
                        url_count += 1

                        # Build the full complaint page URL
                        page_url = i["href"]
                        if not page_url.startswith("http"):
                            page_url = "https://www.bbc.co.uk" + page_url

                        # Process each complaint page
                        pass_complaint_page(page_url, csv_writer)

                # Stop the loop if no more complaint links are found
                if url_count == 0:
                    print("No more complaint pages found.")
                    break

                count += 1  # Move to the next page

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch complaints page: {url} -> {e}")
                break

main()
