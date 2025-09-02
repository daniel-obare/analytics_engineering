-- This file contains a dbt Jinja SQL select statement that uses CTEs for incremental loading from the `client data` BigQuery table.

with source_data as (
    select 
        id,
        name,
        email,
        created_at,
        updated_at
    from 
        `finance.bidev.client_data`
),

incremental_data as (
    select 
        id,
        name,
        email,
        created_at,
        updated_at
    from 
        source_data
    where 
        updated_at > (select max(updated_at) from {{ this }})
)

select 
    id,
    name,
    email,
    created_at,
    updated_at
from 
    incremental_data;