-- Fails (returns rows) for any payment with a zero or negative amount.
select txn_id, amount_minor
from {{ ref('stg_settlements') }}
where amount_minor <= 0
