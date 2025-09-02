# file_picker.py
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_csv_files_from_drive(credentials, folder_id):
    try:
        service = build('drive', 'v3', credentials=credentials)
        query = f"'{folder_id}' in parents and name contains '.csv' and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=100
        ).execute()
        items = results.get('files', [])
        return [(item['id'], item['name']) for item in items]
    except HttpError as error:
        print(f'An error occurred: {error}')
        return []