-- This file creates a dimension table for clients using data from the staging tables.

WITH client_data AS (
    SELECT
        client_id,
        client_name,
        client_email,
        created_at,
        updated_at
    FROM {{ ref('stg_client_data') }}
),

disbursement_data AS (
    SELECT
        client_id,
        SUM(amount) AS total_disbursements
    FROM {{ ref('stg_disbursements') }}
    GROUP BY client_id
),

repayment_data AS (
    SELECT
        client_id,
        SUM(amount) AS total_repayments
    FROM {{ ref('stg_repayments') }}
    GROUP BY client_id
)

SELECT
    c.client_id,
    c.client_name,
    c.client_email,
    c.created_at,
    c.updated_at,
    COALESCE(d.total_disbursements, 0) AS total_disbursements,
    COALESCE(r.total_repayments, 0) AS total_repayments
FROM client_data c
LEFT JOIN disbursement_data d ON c.client_id = d.client_id
LEFT JOIN repayment_data r ON c.client_id = r.client_id