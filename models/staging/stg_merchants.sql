-- One row per merchant with its CURRENT attributes. The snapshot watches this model for changes.
-- Merchants that have transactions but no profile yet get placeholder values.
with latest_profile as (
    select *
    from (
        select
            *,
            row_number() over (partition by merchant_id order by effective_date desc) as rn
        from {{ ref('stg_merchant_profiles') }}
        where effective_date <= current_date
    ) ranked
    where rn = 1
),

all_merchants as (
    select merchant_id from {{ ref('stg_settlements') }}
    union
    select merchant_id from {{ ref('stg_merchant_profiles') }}
)

select
    m.merchant_id,
    coalesce(p.merchant_name, 'Unknown') as merchant_name,
    coalesce(p.category, 'unknown')      as category,
    coalesce(p.risk_tier, 'unrated')     as risk_tier
from all_merchants m
left join latest_profile p on p.merchant_id = m.merchant_id
