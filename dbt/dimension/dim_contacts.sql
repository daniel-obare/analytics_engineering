-- This file contains a dbt Jinja SQL select statement that creates a dimension table for contacts, using data from the staging tables.

WITH source_contacts AS (
    SELECT
        id,
        first_name,
        last_name,
        email,
        phone,
        created_at,
        updated_at
    FROM {{ ref('stg_contacts') }}
),

distinct_contacts AS (
    SELECT
        id,
        first_name,
        last_name,
        email,
        phone,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM source_contacts
)

SELECT
    id,
    first_name,
    last_name,
    email,
    phone,
    created_at,
    updated_at
FROM distinct_contacts
WHERE rn = 1;