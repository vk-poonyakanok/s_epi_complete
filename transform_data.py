import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# Google Drive service build
def google_drive_service():
    print("Building Google Drive service...")
    credentials = Credentials.from_service_account_file('gcp_service_account.json')
    service = build('drive', 'v3', credentials=credentials)
    print("Google Drive service built successfully.")
    return service

# Download file from Google Drive
def download_file(service, file_id, file_path):
    print(f"Downloading file with ID {file_id}...")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    with open(file_path, 'wb') as f:
        f.write(fh.read())
    print("File downloaded successfully.")

# Upload or Update file to Google Drive
def upload_file(service, file_path, folder_id, file_id=None):
    file_metadata = {
        'name': file_path.split('/')[-1],
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='text/csv')
    if file_id:
        # Check if file exists
        print(f"Checking if file with ID {file_id} exists...")
        try:
            service.files().get(fileId=file_id).execute()
            print("File exists, updating...")
            service.files().update(fileId=file_id, body=file_metadata, media_body=media, fields='id').execute()
            print(f"File with ID {file_id} updated successfully.")
        except Exception as e:
            print(f"File does not exist or an error occurred: {e}. Creating new file...")
            file_id = None

    if not file_id:
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"New file created with ID: {file.get('id')}")

def map_column_to_date(b_year, column):
    ce_year = b_year - 543
    month_num = int(column[-2:])
    if month_num >= 10:
        year = ce_year - 1
    else:
        year = ce_year
    return f"{year}-{str(month_num).zfill(2)}-01"

# Transformation logic
def transform_data(file_path):
    print("Transforming data...")
    df = pd.read_csv(file_path)

    # Mapping IDs to names with print step
    print("Mapping IDs to report names...")
    id_to_name = {
        "28dd2c7955ce926456240b2ff0100bde": "1yr",
        "35f4a8d465e6e1edc05f3d8ab658c551": "2yr",
        "d1fe173d08e959397adf34b1d77e88d7": "3yr",
        "f033ab37c30201f73f142449d037028d": "5yr",
        "30f72fc853a2cc02ef953dc97f36f596": "7yr"
    }
    df['report_name'] = df['id'].map(id_to_name)
    print("ID to report name mapping completed.")

    # Converting 'b_year' to integer with print step
    print("Converting budget year to integer...")
    df['b_year'] = df['b_year'].astype(int)
    print("Budget year conversion completed.")

    optimized_df_all = pd.DataFrame()
    print("Starting monthly data transformation...")
    
    for month in range(1, 13):
        print(f"Processing month: {month}")
        target_col = f'target{str(month).zfill(2)}'
        result_col = f'result{str(month).zfill(2)}'
        date_col = df['b_year'].apply(lambda x: map_column_to_date(x, target_col))
        temp_df = df[['report_name', 'hospcode', 'areacode', 'b_year']].copy()
        temp_df['date'] = date_col
        temp_df['target'] = df[target_col]
        temp_df['result'] = df[result_col]
        optimized_df_all = pd.concat([optimized_df_all, temp_df], ignore_index=True)
        print(f"Month {month} processed.")

    print("Monthly data transformation completed.")

    # Cleaning up the data
    print("Cleaning up data...")
    optimized_df_all.dropna(subset=['target', 'result'], inplace=True)
    optimized_df_all['target'] = optimized_df_all['target'].astype(int)
    optimized_df_all['result'] = optimized_df_all['result'].astype(int)
    print("Data cleanup completed.")

    return optimized_df_all

def main():
    service = google_drive_service()
    
    # Download the file
    file_id = '1XTktfgbtlNN4CnsM4BnQ758uwG06uT7y'
    file_path = 's_epi_complete_data_all.csv'
    download_file(service, file_id, file_path)
    
    # Transform the data
    transformed_df = transform_data(file_path)
    
    # Save the transformed data to a new file
    transformed_file_path = 'optimized_s_epi_complete_data_all.csv'
    transformed_df.to_csv(transformed_file_path, index=False)
    print("Transformed file saved.")
    
    # Upload the transformed file
    folder_id = '1kUloOi3JWbV-ukH1OfpvN-S5lKt2_VND'
    file_id = '1Fh6eRGpc3vAWJjwPdk85RXuK6C6NRB1Y'
    upload_file(service, transformed_file_path, folder_id, file_id)

if __name__ == '__main__':
    main()
