{% snapshot merchants_snapshot %}

{{
    config(
        target_schema=target.schema,
        unique_key='merchant_id',
        strategy='check',
        check_cols=['merchant_name', 'category', 'risk_tier']
    )
}}

-- SCD Type 2 in dbt: when a checked column changes, the old row is closed (dbt_valid_to is set)
-- and a new current row is added (dbt_valid_to is null).
select * from {{ ref('stg_merchants') }}

{% endsnapshot %}
