{% snapshot dim_subsidiary %}
{{
    config(
        unique_key="subsidiary_id",
        strategy="timestamp",
        updated_at="silver_load_date"
    )
}}

select
    subsidiary_id,
    currency_id,
    subsidiary_full_name,
    is_inactive,
    subsidiary_name,
    parent_id,
    silver_load_date


  from {{ ref('subsidiaries') }}

{% endsnapshot %}

