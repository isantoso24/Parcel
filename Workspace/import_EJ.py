import requests
import pdfplumber
import re
import csv
import os

# List of PDF URLs
pdf_urls = [
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-91.09906537,30.24368202",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-96.61444604,32.30630398",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-88.6461465,31.674883",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-83.81408212,32.01751208",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-95.92398275,41.21960529",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-94.85483234,39.7548294",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-90.05483043,35.06121425",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-96.5946774,32.30461662",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-101.8247348,33.55894605",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-78.17946394,35.78405071",
    "https://ejscreen.epa.gov/mapper/demogreportpdf.aspx?feattype=point&radius=3.0&coords=-89.13313088,41.02285227"
]

# Download PDFs and process
def download_pdf(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        return True
    return False

def extract_data_from_pdf(pdf_path):
    # Default data template
    data = {
        "Population": None,
        "Percent POC": None,
        "Percent 0-17": None,
        "Per Capita Income": None
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue  # Skip if no text is found
            
            # Extract Population (targeting "Summary of ACS Estimates" section)
            population_match = re.search(r"Summary of ACS Estimates.*?Population\s*\n*\s*([\d,]+)", text, re.DOTALL | re.IGNORECASE)
            if population_match:
                data["Population"] = population_match.group(1).replace(",", "")

            # Extract Percent POC
            poc_match = re.search(r"% People of Color Population\s*\n*\s*(\d+)%", text, re.IGNORECASE)
            if poc_match:
                data["Percent POC"] = f"{poc_match.group(1)}%"

            # Extract Percent 0-17 (second column after "Age 0-17")
            age_0_17_match = re.search(r"Age 0-17\s*\n*\s*\d+\s+(\d+)%", text, re.IGNORECASE)
            if age_0_17_match:
                data["Percent 0-17"] = f"{age_0_17_match.group(1)}%"

            # Extract Per Capita Income
            income_match = re.search(r"Per Capita Income\s*\n*\s*([\d,]+)", text, re.IGNORECASE)
            if income_match:
                data["Per Capita Income"] = income_match.group(1).replace(",", "")

    return data

# Main processing function
def process_pdfs(pdf_urls, output_csv):
    # Create temporary folder for PDFs
    os.makedirs("pdf_temp", exist_ok=True)

    # List to hold all extracted data
    extracted_data = []

    for i, url in enumerate(pdf_urls):
        print(f"Processing PDF {i+1} of {len(pdf_urls)}...")
        filename = f"pdf_temp/report_{i+1}.pdf"

        # Download the PDF
        if download_pdf(url, filename):
            print(f"Downloaded PDF {i+1}.")
            # Extract data
            data = extract_data_from_pdf(filename)
            data["PDF URL"] = url  # Add the PDF URL for reference
            extracted_data.append(data)
        else:
            print(f"Failed to download PDF {i+1}.")
    
    # Write data to CSV
    with open(output_csv, mode='w', newline='') as csv_file:
        fieldnames = ["PDF URL", "Population", "Percent POC", "Percent 0-17", "Per Capita Income"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in extracted_data:
            writer.writerow(row)

    print(f"Data extraction complete. Output saved to '{output_csv}'.")

# Run the script
if __name__ == "__main__":
    output_csv_file = "extracted_data.csv"
    process_pdfs(pdf_urls, output_csv_file)
