{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- One row per payment. Incremental: after the first run only rows ingested since the last run are
-- processed. ">=" re-reads the newest batch and unique_key replaces it, so a rerun never duplicates.
select
    txn_id,
    merchant_id,
    account_id,
    created_date,
    settled_date,
    status,
    status = 'settled'                                         as is_settled,
    status = 'reversed'                                        as is_reversed,
    currency,
    amount_minor,
    amount,
    case when status = 'settled' then amount_minor else 0 end  as net_amount_minor,
    extract(epoch from (settled_at - created_at)) / 3600.0     as settlement_lag_hours,
    ingested_at
from {{ ref('stg_settlements') }}

{% if is_incremental() %}
where ingested_at >= (select coalesce(max(ingested_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
