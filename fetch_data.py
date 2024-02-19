import requests
import pandas as pd
import json

# Base URL for the RESTful Web Service
url = "https://opendata.moph.go.th/api/report_data"

# Headers indicating that the payload is in JSON format
headers = {"Content-Type": "application/json"}

# List of all province codes excluding specific ones
excluded_provinces = ['10', '28', '29', '59', '68', '69', '78', '79', '87', '88', '89']
province_codes = [f"{i:02d}" for i in range(11, 97) if f"{i:02d}" not in excluded_provinces]

# Initialize a DataFrame to store all data
s_epi_complete_data = pd.DataFrame()

for province_code in province_codes:
    # Data payload for the POST request
    data = {
        "tableName": "s_epi_complete",
        "year": "2567",  # Specify other years as needed
        "province": province_code,
        "type": "json"
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # Check if the request was successful
    if response.status_code in [200, 201]:
        # Convert the JSON response to a pandas DataFrame and append to the s_epi_complete_data DataFrame
        temp_df = pd.json_normalize(response.json())
        s_epi_complete_data = pd.concat([s_epi_complete_data, temp_df], ignore_index=True)
    else:
        print(f"Failed to retrieve data for province code {province_code}. Status code: {response.status_code}")

# After collecting all data, you can save this DataFrame to a CSV file, a database, or another storage solution.
# Example of saving to CSV (optional):
# s_epi_complete_data.to_csv('s_epi_complete_data.csv', index=False)