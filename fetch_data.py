import requests
import pandas as pd
import json
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def map_column_to_date(b_year, column):
    ce_year = b_year - 543  # Convert the Buddhist Era (BE) year to Common Era (CE) by subtracting 543
    month_num = int(column[-2:])  # Extract the month number from the column name
    if month_num >= 10:  # For 'target10', 'target11', and 'target12', the year is the previous CE year
        year = ce_year - 1
    else:  # For 'target01' to 'target09', it's the same CE year
        year = ce_year
    return f"{year}-{str(month_num).zfill(2)}-01"  # Return the date in 'YYYY-MM-DD' (ISO8601) format

def fetch_data_and_save():
    # Calculate the current year in the Buddhist Era
    current_year_be = datetime.now().year + 543

    url = "https://opendata.moph.go.th/api/report_data"
    headers = {"Content-Type": "application/json"}
    excluded_provinces = ['10', '28', '29', '59', '68', '69', '78', '79', '87', '88', '89']
    province_codes = [f"{i:02d}" for i in range(11, 97) if f"{i:02d}" not in excluded_provinces]
    s_epi_complete_data = pd.DataFrame()

    for province_code in province_codes:
        data = {
            "tableName": "s_epi_complete",
            "year": str(current_year_be),
            "province": province_code,
            "type": "json"
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code in [200, 201]:
            temp_df = pd.json_normalize(response.json())
            s_epi_complete_data = pd.concat([s_epi_complete_data, temp_df], ignore_index=True)
        else:
            print(f"Failed to retrieve data for province code {province_code}. Status code: {response.status_code}")

    # Updating the id_to_name dictionary with English descriptions
    id_to_name = {
        "28dd2c7955ce926456240b2ff0100bde": "1yr",
        "35f4a8d465e6e1edc05f3d8ab658c551": "2yr",
        "d1fe173d08e959397adf34b1d77e88d7": "3yr",
        "f033ab37c30201f73f142449d037028d": "5yr",
        "30f72fc853a2cc02ef953dc97f36f596": "7yr"
    }
    
    # Mapping the 'id' column to the new short report names using 'id_to_name' dictionary
    s_epi_complete_data['report_name'] = s_epi_complete_data['id'].map(id_to_name)
    
    # Proceed with the optimized data transformation
    s_epi_complete_data['b_year'] = s_epi_complete_data['b_year'].astype(int)
    optimized_df = pd.DataFrame()
    
    for month in range(1, 13):
        target_col = f'target{str(month).zfill(2)}'
        result_col = f'result{str(month).zfill(2)}'
        date_col = s_epi_complete_data['b_year'].apply(lambda x: map_column_to_date(x, target_col))
        temp_df = s_epi_complete_data[['report_name', 'hospcode', 'areacode', 'b_year']].copy()
        temp_df['date'] = date_col
        temp_df['target'] = s_epi_complete_data[target_col]
        temp_df['result'] = s_epi_complete_data[result_col]
        optimized_df = pd.concat([optimized_df, temp_df], ignore_index=True)
    
    # Finalize the optimized DataFrame
    optimized_df.dropna(subset=['target', 'result'], inplace=True)
    optimized_df['target'] = optimized_df['target'].astype(int)
    optimized_df['result'] = optimized_df['result'].astype(int)

    # Export the optimized DataFrame to a CSV file
    optimized_df.to_csv('optimized_s_epi_complete_data.csv', index=False)
    print("Optimized data transformation and saving complete.")

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
    # Specify the existing file ID here to update the file ID '1smFzFo3YSWQ_xtEMHr2ACxKc0KnYX0gA' instead of uploading as new
    upload_to_drive('optimized_s_epi_complete_data.csv', '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND', '1smFzFo3YSWQ_xtEMHr2ACxKc0KnYX0gA')
