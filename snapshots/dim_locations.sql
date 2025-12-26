{% snapshot dim_locations
 %}
{{
    config(
        unique_key="location_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    location_id,
    location_name,
    location_full_name,
    is_inactive,
    last_modified_date,
    latitude,
    longitude,
    location_type,
    parent,
    subsidiary_id

  from {{ ref('locations') }}

{% endsnapshot %}

