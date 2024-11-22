from PyPDF2 import PdfReader
import regex as re
import os

# Function to extract text and structured data from a PDF
def extract_text_from_pdf(pdf_path):
    reader = PdfReader(pdf_path)

    # Extract text
    extracted_text = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            page_text = page.extract_text()
            if page_text:
                extracted_text.append(page_text)
            else:
                print(f"Warning: No text found on page {i} in {pdf_path}")
        except Exception as e:
            print(f"Error extracting text from page {i} in {pdf_path}: {e}")

    # Combine all text into a single string
    text = "\n".join(extracted_text)
    print(text)
    return None

text =""
pdf_folder =  "/home/lucy/Desktop/transphobse/data/bbc_complaint_pdfs/"
for file_name in os.listdir(pdf_folder):
    if file_name.endswith(".pdf") and ("ecu" in file_name):
        pdf_path = os.path.join(pdf_folder, file_name)
        file_data = extract_text_from_pdf(pdf_path)
        if file_data:
            text += file_data + "\n"
print(text)