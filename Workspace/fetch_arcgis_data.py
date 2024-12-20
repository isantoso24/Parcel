import requests
import json
import time
import zipfile
import os

# Base URL and parameters
url = "https://fs.regrid.com/otfjlL4RwGHNLiXOueo02yh31UwdJlMcPC8kYQjYcW2GKsxrOR2tA6Ia37GOndlH/rest/services/premium/FeatureServer/0/query"
params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "resultOffset": 0,  # Will be updated from progress.txt
    "resultRecordCount": 5000  # Fetch in manageable chunks
}

# Settings
file_count = 1  # Start file index
features_per_zip = 10_000_000  # Number of features per ZIP file
MAX_RETRIES = 5

progress_file = "progress.txt"  # File to save progress

def load_progress():
    """Load last saved offset and file count."""
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            offset, count = map(int, f.read().split(","))
            print(f"Resuming from offset {offset}, file index {count}")
            return offset, count
    return 0, 1  # Start from scratch

def save_progress(offset, file_index):
    """Save current progress to a file."""
    with open(progress_file, "w") as f:
        f.write(f"{offset},{file_index}")

def fetch_data_with_retries(url, params, retries=MAX_RETRIES):
    """Fetch data with retry mechanism."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print("Max retries reached. Skipping this batch.")
                return None

def save_and_zip(features, file_index):
    """Save features to a geojson file and compress it as ZIP."""
    geojson_file = f"output_chunk_{file_index}.geojson"
    zip_file = f"output_chunk_{file_index}.zip"

    # Save to GeoJSON
    with open(geojson_file, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
    print(f"Saved {len(features)} features to {geojson_file}")

    # Compress to ZIP
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(geojson_file)
    print(f"Compressed {geojson_file} to {zip_file}")

    # Clean up original geojson
    os.remove(geojson_file)
    print(f"Deleted {geojson_file} to save space.")

# Load progress
params['resultOffset'], file_count = load_progress()

# Main loop
features_in_current_zip = []
while True:
    print(f"Fetching records starting at offset {params['resultOffset']}...")
    data = fetch_data_with_retries(url, params)
    
    if not data:
        print("Error fetching data. Moving to next batch.")
        params['resultOffset'] += params['resultRecordCount']
        save_progress(params['resultOffset'], file_count)
        continue

    features = data.get('features', [])
    if not features:
        print("No more features found. Exiting loop.")
        break

    # Append features to the current list
    features_in_current_zip.extend(features)

    # Save and ZIP once threshold is reached
    if len(features_in_current_zip) >= features_per_zip:
        save_and_zip(features_in_current_zip, file_count)
        features_in_current_zip = []  # Reset for the next ZIP file
        file_count += 1
        save_progress(params['resultOffset'], file_count)  # Save progress

        # Pause for user confirmation
        print("Paused! Download the ZIP file and delete it to free up space.")
        input("Press ENTER to continue...")

    # Update offset for next batch
    params['resultOffset'] += params['resultRecordCount']
    save_progress(params['resultOffset'], file_count)  # Save progress
    time.sleep(1)  # Delay to avoid overloading server

# Save remaining features
if features_in_current_zip:
    save_and_zip(features_in_current_zip, file_count)
    print(f"Saved remaining features to chunk {file_count}.")
    save_progress(params['resultOffset'], file_count + 1)

print("Data retrieval and compression complete.")
