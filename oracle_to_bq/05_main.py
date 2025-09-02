# main.py
import os
from google.cloud import bigquery
from secrets import GOOGLE_CREDENTIALS_PATH, GCP_PROJECT_ID
from get_last_load_date import get_last_load_date
from fetch_incremental_data import fetch_incremental_data
from load_to_bigquery import load_to_bigquery

def main():
    # Set up Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDENTIALS_PATH
    
    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Get the last load date
    last_load_date = get_last_load_date(client)
    
    # Fetch incremental data from Oracle
    df = fetch_incremental_data(last_load_date)
    
    # Load to BigQuery if data exists
    if not df.empty:
        load_to_bigquery(df, client)
    else:
        print("No incremental data found.")

if __name__ == "__main__":
    main()