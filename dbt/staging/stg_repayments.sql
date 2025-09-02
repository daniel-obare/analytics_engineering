-- Incremental loading for repayments from the BigQuery table
with source_data as (
    select
        id,
        amount,
        repayment_date,
        client_id,
        created_at,
        updated_at
    from
        `finance.bidev.repayments`
    where
        repayment_date > (select max(repayment_date) from {{ this }})
),

filtered_data as (
    select
        id,
        amount,
        repayment_date,
        client_id,
        created_at,
        updated_at
    from
        source_data
    where
        id is not null
)

select
    id,
    amount,
    repayment_date,
    client_id,
    created_at,
    updated_at
from
    filtered_data;