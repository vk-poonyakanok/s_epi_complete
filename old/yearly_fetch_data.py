import requests
import pandas as pd
import json
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def fetch_data_and_save():
    # Calculate the current year in the Buddhist Era
    current_year_be = datetime.now().year + 543

    url = "https://opendata.moph.go.th/api/report_data"
    headers = {"Content-Type": "application/json"}
    excluded_provinces = ['10', '28', '29', '59', '68', '69', '78', '79', '87', '88', '89']
    province_codes = [f"{i:02d}" for i in range(11, 97) if f"{i:02d}" not in excluded_provinces]
    
    s_epi_complete_data_all = pd.DataFrame()

    # Iterate through the years from 2557 to the last year (excluding the current year)
    for year in range(2557, current_year_be):  # Exclude current year (If want to include the current year use current_year_be +1)
        for province_code in province_codes:
            data = {
                "tableName": "s_epi_complete",
                "year": str(year),
                "province": province_code,
                "type": "json"
            }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            if response.status_code in [200, 201]:
                temp_df = pd.json_normalize(response.json())
                s_epi_complete_data_all = pd.concat([s_epi_complete_data_all, temp_df], ignore_index=True)
            else:
                print(f"Failed to retrieve data for year {year}, province code {province_code}. Status code: {response.status_code}")

    s_epi_complete_data_all.to_csv('s_epi_complete_data_all.csv', index=False)
    print("Yearly data fetching and saving complete.")

def upload_to_drive(filename, folder_id, file_id=None):
    service_account_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    service = build('drive', 'v3', credentials=credentials)

    media = MediaFileUpload(filename, mimetype='text/csv')

    if file_id:
        # Update the existing file
        updated_file = service.files().update(
            fileId=file_id,
            media_body=media,
            fields='id'
        ).execute()
        print(f"Updated {filename} in Google Drive with File ID: {updated_file.get('id')}")
    else:
        # Create a new file if no file_id is provided
        file_metadata = {'name': os.path.basename(filename), 'parents': [folder_id]}
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"Uploaded {filename} to Google Drive with File ID: {file.get('id')}")

if __name__ == '__main__':
    fetch_data_and_save()
    # Upload to Google Drive folder ID '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND'
    # Specify the existing file ID here to update the file ID '1XTktfgbtlNN4CnsM4BnQ758uwG06uT7y' instead of uploading as new
    upload_to_drive('s_epi_complete_data_all.csv', '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND', '1XTktfgbtlNN4CnsM4BnQ758uwG06uT7y')

