# get_last_load_date.py
from google.cloud import bigquery
from datetime import datetime, timedelta
from secrets import GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE

def get_last_load_date(client):
    """
    Fetch the maximum last_updated date from the BigQuery table.
    Returns the date or defaults to 30 days ago if no data exists.
    """
    query = f"""
    SELECT MAX(last_updated) as last_load_date
    FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    """
    query_job = client.query(query)
    result = query_job.to_dataframe()
    last_load_date = result['last_load_date'].iloc[0]
    # If no data exists, default to 30 days ago
    return last_load_date if last_load_date else (datetime.now() - timedelta(days=30)).date()