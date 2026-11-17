-- Cleaned merchant profile feed: one row per merchant per effective date.
select
    merchant_id,
    trim(merchant_name)       as merchant_name,
    lower(trim(category))     as category,
    lower(trim(risk_tier))    as risk_tier,
    effective_date
from {{ ref('merchant_profiles') }}
