import pandas as pd
import io
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

def transform_data(credentials, file_ids):
    dfs = []
    service = build('drive', 'v3', credentials=credentials)
    
    for file_id, file_name in file_ids:
        try:
            request = service.files().get_media(fileId=file_id)
            content = request.execute()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            df = pd.read_csv(io.StringIO(content))
            dfs.append(df)
        except HttpError as error:
            print(f'An error occurred while downloading {file_name}: {error}')
    
    if not dfs:
        raise ValueError("No CSV files found or loaded.")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    today = datetime.now().strftime('%Y%m%d')  # Dynamic date in YYYYMMDD format
    combined_df['id_date'] = int(today)  # Convert to integer for numeric format
    
    return combined_df