import requests
import pandas as pd
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def fetch_data_and_save():
    url = "https://opendata.moph.go.th/api/report_data"
    headers = {"Content-Type": "application/json"}
    excluded_provinces = ['10', '28', '29', '59', '68', '69', '78', '79', '87', '88', '89']
    province_codes = [f"{i:02d}" for i in range(11, 97) if f"{i:02d}" not in excluded_provinces]
    s_epi_complete_data = pd.DataFrame()

    for province_code in province_codes:
        data = {
            "tableName": "s_epi_complete",
            "year": "2567",
            "province": province_code,
            "type": "json"
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code in [200, 201]:
            temp_df = pd.json_normalize(response.json())
            s_epi_complete_data = pd.concat([s_epi_complete_data, temp_df], ignore_index=True)
        else:
            print(f"Failed to retrieve data for province code {province_code}. Status code: {response.status_code}")

    s_epi_complete_data.to_csv('s_epi_complete_data.csv', index=False)
    print("Data fetching and saving complete.")

def upload_to_drive(filename, folder_id):
    service_account_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    service = build('drive', 'v3', credentials=credentials)

    # Search for the file in the specified folder
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])

    file_metadata = {'name': filename, 'parents': [folder_id]}
    media = MediaFileUpload(filename, mimetype='text/csv')

    if files:
        # If file exists, update it
        file_id = files[0].get('id')
        updated_file = service.files().update(fileId=file_id, body=file_metadata, media_body=media).execute()
        print(f"Updated {filename} in Google Drive with File ID: {updated_file.get('id')}")
    else:
        # If file does not exist, create it
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded {filename} to Google Drive with File ID: {file.get('id')}")

if __name__ == '__main__':
    # Upload to Google Drive folder ID 1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND
    upload_to_drive('s_epi_complete_data.csv', '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND')