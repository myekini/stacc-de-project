-- One row per raw settlement, typed and renamed. No business logic here.
select
    txn_id,
    merchant_id,
    account_id,
    amount_minor,
    round(amount_minor / 100.0, 2)                 as amount,
    upper(currency)                                as currency,
    status,
    created_at,
    (created_at at time zone 'UTC')::date          as created_date,
    settled_at,
    (settled_at at time zone 'UTC')::date          as settled_date,
    ingested_at
from {{ source('raw', 'raw_settlements') }}
