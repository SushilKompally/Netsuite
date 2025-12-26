{% snapshot dim_departments %}
{{
    config(
        unique_key="department_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    department_id,
    department_name,
    department_full_name,
    is_inactive,
    parent,
    last_modified_date,

  from {{ ref('departments') }}

{% endsnapshot %}

