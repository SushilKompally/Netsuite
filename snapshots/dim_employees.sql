{% snapshot dim_employees %}
{{
    config(
        unique_key="employee_id",
        strategy="timestamp",
        updated_at="last_modified_date"
    )
}}

select
    EMPLOYEE_ID,
    ENTITY_ID,
    EMAIL,
    TITLE,
    JOB_DESCRIPTION,
    CLASS_ID,
    CURRENCY,
    DEPARTMENT_ID,
    DATE_CREATED,
    EMPLOYEE_TYPE_ID,
    ACCOUNT_NUMBER,
    LOCATION_ID,
    EMPLOYEE_STATUS_ID,
    IS_INACTIVE,
    SUBSIDIARY,
    LAST_MODIFIED_DATE,

  from {{ ref('employees') }}

{% endsnapshot %}

