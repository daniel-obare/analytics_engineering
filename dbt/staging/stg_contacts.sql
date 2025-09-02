-- Incremental loading of contacts data from the BigQuery table
with source_data as (
    select
        id,
        name,
        email,
        phone,
        created_at,
        updated_at
    from
        `finance.bidev.contacts`
    where
        updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
),

filtered_data as (
    select
        id,
        name,
        email,
        phone,
        created_at,
        updated_at
    from
        source_data
)

select
    id,
    name,
    email,
    phone,
    created_at,
    updated_at
from
    filtered_data