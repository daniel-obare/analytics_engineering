# load_to_bigquery.py
from google.cloud import bigquery
from secrets import GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE

def load_to_bigquery(df, client):
    """
    Load DataFrame to BigQuery table in append mode.
    """
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    
    # Load job configuration (append mode for incremental loads)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing table
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]  # Allow adding new columns
    )
    
    # Load DataFrame to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    print(f"Loaded {len(df)} rows to {table_id}")