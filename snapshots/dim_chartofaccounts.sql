{% snapshot dim_chartofaccounts %}
{{
    config(
        unique_key="account_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    a.account_id,
    a.class_id,
    coalesce(a.currency_id, s.currency_id)          as currency_id,
    a.department_id,
    a.location_id,
    a.parent_id                                      as account_parent_id,
    coalesce(a.subsidiary_id, s.subsidiary_id)      as subsidiary_id,
    a.account_number,
    a.account_description,
    a.display_name,
    a.display_name_with_hierarchy,
    a.account_name,
    a.account_type,
    s.subsidiary_name,
    s.subsidiary_full_name,
    s.parent_id                                      as subsidiary_parent_id,
    c.class_full_name,
    c.class_name,
    c.parent                                         as classification_parent,
    a.last_modified_date


  from {{ ref('accounts') }} a
  left join {{ ref('classification') }} c
    on a.class_id = c.class_id
  left join {{ ref('subsidiaries') }} s
    on a.subsidiary_id = s.subsidiary_id


{% endsnapshot %}

