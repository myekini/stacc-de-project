-- Fails if the incremental mart and staging disagree on row count or total amount.
with mart as (
    select count(*) as n, coalesce(sum(amount_minor), 0) as total from {{ ref('fct_payments') }}
), stg as (
    select count(*) as n, coalesce(sum(amount_minor), 0) as total from {{ ref('stg_settlements') }}
)
select mart.n as mart_rows, stg.n as stg_rows, mart.total as mart_total, stg.total as stg_total
from mart, stg
where mart.n <> stg.n or mart.total <> stg.total
