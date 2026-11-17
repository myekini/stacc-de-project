-- Fails (returns rows) if the profile feed has two rows for the same merchant and date.
select merchant_id, effective_date, count(*) as n
from {{ ref('stg_merchant_profiles') }}
group by merchant_id, effective_date
having count(*) > 1
