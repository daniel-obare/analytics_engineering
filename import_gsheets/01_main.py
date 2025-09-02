# main.py
from secrets import get_service_account_credentials, get_db_connection_string, DRIVE_FOLDER_ID
from file_picker import get_csv_files_from_drive
from transformer import transform_data
from loader import load_to_database

def main():
    # Get credentials and folder ID from secrets
    credentials = get_service_account_credentials()
    db_conn_str = get_db_connection_string()
    
    # Step 1: Get list of CSV files from Google Drive folder
    csv_file_ids = get_csv_files_from_drive(credentials, DRIVE_FOLDER_ID)
    
    # Step 2: Transform data (read, append, add id_date)
    df = transform_data(credentials, csv_file_ids)
    
    # Step 3: Load to database
    load_to_database(df, db_conn_str, table_name='disbursement')

if __name__ == '__main__':
    main()