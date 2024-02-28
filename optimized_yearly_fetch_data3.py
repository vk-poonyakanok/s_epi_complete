import requests
import pandas as pd
import json
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def log(message):
    print(message)

def map_column_to_date(b_year, column):
    try:
        ce_year = b_year - 543
        month_num = int(column[-2:])
        year = ce_year - 1 if month_num >= 10 else ce_year
        return f"{year}-{str(month_num).zfill(2)}-01"
    except Exception as e:
        log(f"Error in map_column_to_date: {e}")
        raise

def fetch_data_for_province(year, province_code):
    url = "https://opendata.moph.go.th/api/report_data"
    headers = {"Content-Type": "application/json"}
    data = {
        "tableName": "s_epi_complete",
        "year": str(year),
        "province": province_code,
        "type": "json"
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code in [200, 201]:
        temp_df = pd.json_normalize(response.json())
        return temp_df
    else:
        log(f"Failed to retrieve data for year {year}, province code {province_code}. Status code: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame in case of failure
    
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_all_data():
    current_year_be = datetime.now().year + 543
    excluded_provinces = ['10', '28', '29', '59', '68', '69', '78', '79', '87', '88', '89']
    province_codes = [f"{i:02d}" for i in range(11, 97) if f"{i:02d}" not in excluded_provinces]
    # years = range(2557, 2558) # This comment is for testing
    years = range(2557, current_year_be)

    all_data = pd.DataFrame()
    
    # Parallel Data Fetching
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Create a list to hold futures
        futures = []
        for year in years:
            for province_code in province_codes:
                futures.append(executor.submit(fetch_data_for_province, year, province_code))

        # As each future completes, concatenate its result
        for future in as_completed(futures):
            all_data = pd.concat([all_data, future.result()], ignore_index=True)

    return all_data

def transform_data2(s_epi_complete_data_all):
    try:
        log("Starting data transformation...")
        id_to_name = {
            "28dd2c7955ce926456240b2ff0100bde": "1yr",
            "35f4a8d465e6e1edc05f3d8ab658c551": "2yr",
            "d1fe173d08e959397adf34b1d77e88d7": "3yr",
            "f033ab37c30201f73f142449d037028d": "5yr",
            "30f72fc853a2cc02ef953dc97f36f596": "7yr"
        }

        # Vectorized operation
        s_epi_complete_data_all['report_name'] = pd.Categorical(s_epi_complete_data_all['id'].map(id_to_name))

        s_epi_complete_data_all['b_year'] = s_epi_complete_data_all['b_year'].astype(int)

        optimized_df_all = pd.DataFrame()

        for month in range(1, 13):
            target_col = f'target{str(month).zfill(2)}'
            result_col = f'result{str(month).zfill(2)}'
            date_col = s_epi_complete_data_all['b_year'].apply(lambda x: map_column_to_date(x, target_col))
            temp_df = s_epi_complete_data_all[['report_name', 'hospcode', 'areacode', 'b_year']].copy()
            temp_df['date'] = date_col
            temp_df['target'] = s_epi_complete_data_all[target_col]
            temp_df['result'] = s_epi_complete_data_all[result_col]
            optimized_df_all = pd.concat([optimized_df_all, temp_df], ignore_index=True)

        optimized_df_all.dropna(subset=['target', 'result'], inplace=True)
        optimized_df_all['target'] = optimized_df_all['target'].astype(int)
        optimized_df_all['result'] = optimized_df_all['result'].astype(int)

        log("Data transformation completed successfully.")
        return optimized_df_all  # Ensure the return statement is within the try block.
    except Exception as e:
        log(f"Error during data transformation: {e}")
        raise

def save_transformed_data(optimized_df_all, filename='optimized_s_epi_complete_data_all.csv'):
    try:
        log("Saving transformed data to CSV...")
        optimized_df_all.to_csv(filename, index=False)
        log("Optimized yearly data transformation and saving complete.")
    except Exception as e:
        log(f"Error saving transformed data: {e}")
        raise

def upload_to_drive(filename, folder_id, file_id=None):
    try:
        log("Starting upload to Google Drive...")
        service_account_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        credentials = service_account.Credentials.from_service_account_file(service_account_file)
        service = build('drive', 'v3', credentials=credentials)
        media = MediaFileUpload(filename, mimetype='text/csv')

        if file_id:
            updated_file = service.files().update(fileId=file_id, media_body=media, fields='id').execute()
            log(f"Updated {filename} in Google Drive with File ID: {updated_file.get('id')}")
        else:
            file_metadata = {'name': os.path.basename(filename), 'parents': [folder_id]}
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            log(f"Uploaded {filename} to Google Drive with File ID: {file.get('id')}")

    except Exception as e:
        log(f"Error during upload to Google Drive: {e}")
        raise

if __name__ == '__main__':
    try:
        s_epi_complete_data_all = fetch_all_data()
        optimized_df_all = transform_data2(s_epi_complete_data_all)
        save_transformed_data(optimized_df_all)  # Call the new function to save the data
        # Specify the folder ID and file ID for Google Drive upload
        upload_to_drive('optimized_s_epi_complete_data_all.csv', '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND', '1Fh6eRGpc3vAWJjwPdk85RXuK6C6NRB1Y')
    except Exception as e:
        log(f"Unexpected error in main: {e}")
