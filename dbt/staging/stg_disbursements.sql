-- Incremental loading of disbursements data using CTEs

with source_data as (
    select *
    from `finance.bidev.disbursements`
    where updated_at > (select max(updated_at) from {{ this }})
),

filtered_data as (
    select
        id,
        amount,
        disbursement_date,
        client_id,
        created_at,
        updated_at
    from source_data
)

select
    id,
    amount,
    disbursement_date,
    client_id,
    created_at,
    updated_at
from filtered_data