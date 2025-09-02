-- This file contains a dbt Jinja SQL select statement that aggregates data from the staging tables to create a fact table for loans.

WITH disbursement_data AS (
    SELECT
        id,
        client_id,
        amount,
        disbursement_date,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY disbursement_date DESC) AS rn
    FROM {{ ref('stg_disbursements') }}
),

repayment_data AS (
    SELECT
        id,
        client_id,
        amount,
        repayment_date,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY repayment_date DESC) AS rn
    FROM {{ ref('stg_repayments') }}
),

combined_data AS (
    SELECT
        d.client_id,
        SUM(d.amount) AS total_disbursements,
        SUM(r.amount) AS total_repayments
    FROM disbursement_data d
    LEFT JOIN repayment_data r ON d.client_id = r.client_id
    WHERE d.rn = 1 OR r.rn = 1
    GROUP BY d.client_id
)

SELECT
    client_id,
    total_disbursements,
    total_repayments
FROM combined_data;