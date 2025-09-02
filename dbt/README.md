# dbt Project Documentation

This dbt project is designed to facilitate the transformation and loading of data from BigQuery tables into a structured format suitable for analytics. The project is organized into three main categories: staging, fact, and dimension tables.

## Folder Structure

- **staging/**: Contains staging models that perform incremental loads from the source BigQuery tables.
  - `stg_disbursements.sql`: Incrementally loads data from the `disbursements` table.
  - `stg_repayments.sql`: Incrementally loads data from the `repayments` table.
  - `stg_contacts.sql`: Incrementally loads data from the `contacts` table.
  - `stg_client_data.sql`: Incrementally loads data from the `client data` table.

- **fact/**: Contains fact models that aggregate data from the staging tables.
  - `fact_loans.sql`: Creates a fact table for loans by aggregating data from the staging tables.

- **dimension/**: Contains dimension models that provide descriptive attributes related to the facts.
  - `dim_clients.sql`: Creates a dimension table for clients using data from the staging tables.
  - `dim_contacts.sql`: Creates a dimension table for contacts using data from the staging tables.

## Instructions

1. **Setup dbt**: Ensure you have dbt installed and configured to connect to your BigQuery project.
2. **Run Models**: Use the following command to run all models:
   ```
   dbt run
   ```
3. **Incremental Loading**: The staging models are designed to perform incremental loading, which means they will only load new or updated records since the last run. Ensure that your source tables have a timestamp or unique identifier to facilitate this.

4. **Documentation**: To generate documentation for your dbt models, run:
   ```
   dbt docs generate
   dbt docs serve
   ```

This project aims to streamline the ETL process and provide a robust framework for data analysis.