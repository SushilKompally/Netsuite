{% snapshot dim_accounting_period %}
{{
    config(
        unique_key="posting_period_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    posting_period_id,
    closed_on_date,
    last_modified_date,
    end_date,
    start_date,
    period_name,
    year,
    silver_load_date
from {{ ref("accounting_period") }}

{% endsnapshot %}

