import requests
import json

# Base URL and parameters
url = "https://fs.regrid.com/otfjlL4RwGHNLiXOueo02yh31UwdJlMcPC8kYQjYcW2GKsxrOR2tA6Ia37GOndlH/rest/services/premium/FeatureServer/0/query"
params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "resultOffset": 0,
    "resultRecordCount": 500  # Reduced batch size
}

all_features = []

# Loop through the data in pages
while True:
    print(f"Fetching records starting at offset {params['resultOffset']}...")
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print("Error occurred:", response.text)  # Print error details
        break
    
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Failed to parse JSON response.")
        break
    
    features = data.get('features', [])
    if not features:
        print("No more features found. Exiting loop.")
        break  # Stop if no more features
    
    all_features.extend(features)
    params['resultOffset'] += params['resultRecordCount']  # Move to next page

# Save all data to a file
if all_features:
    with open("output.geojson", "w") as f:
        json.dump({"type": "FeatureCollection", "features": all_features}, f)
    print("Data retrieval complete. Saved to output.geojson.")
else:
    print("No data was retrieved.")
