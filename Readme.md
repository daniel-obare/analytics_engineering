# Python Analytics Project

This repository contains two main ETL pipelines for data integration and analytics:

## 1. Google Sheets Import (`import_gsheets`)

This module automates the process of fetching CSV files from a Google Drive folder, transforming the data, and loading it into a database.

**Workflow:**
- **Authentication:** Uses service account credentials to access Google Drive.
- **File Selection:** [`get_csv_files_from_drive`](import_gsheets/03_file_picker.py) lists all CSV files in a specified folder.
- **Transformation:** [`transform_data`](import_gsheets/04_transform.py) reads and concatenates CSVs, adding a dynamic `id_date` column.
- **Loading:** [`load_to_database`](import_gsheets/05_load_db.py) loads the transformed data into a database table.

**Entry Point:**  
Run [`01_main.py`](import_gsheets/01_main.py) to execute the full pipeline.

## 2. Oracle to BigQuery (`oracle_to_bq`)

This module extracts incremental data from an Oracle database and loads it into a BigQuery table.

**Workflow:**
- **Secrets Management:** Stores credentials and configuration in [`01_secrets.py`](oracle_to_bq/01_secrets.py).
- **Last Load Date:** [`get_last_load_date`](oracle_to_bq/02_get_last_load_date.py) fetches the latest update timestamp from BigQuery.
- **Incremental Fetch:** [`fetch_incremental_data`](oracle_to_bq/03_fetch_incremental_data.py) retrieves new records from Oracle since the last load.
- **Loading:** [`load_to_bigquery`](oracle_to_bq/04_load_to_bq.py) appends new data to BigQuery.
- **Orchestration:** [`05_main.py`](oracle_to_bq/05_main.py) coordinates the ETL steps.

**Entry Point:**  
Run [`05_main.py`](oracle_to_bq/05_main.py) to execute the Oracle-to-BigQuery pipeline.

---

## Requirements

- Python 3.x
- Google Cloud SDK
- Oracle client libraries
- SQLAlchemy (for database loading)
- pandas

## Usage

1. Configure secrets in the respective `*_secrets.py` files.
2. Install dependencies:  
   `pip install -r requirements.txt`
3. Run the desired pipeline:
   - Google Sheets: `python import_gsheets/01_main.py`
   - Oracle to BigQuery: `python oracle_to_bq/05_main.py`

## Folder Structure

- `import_gsheets/` — Google Drive to DB ETL
- `oracle_to_bq/` — Oracle to BigQuery ETL

See individual scripts for more