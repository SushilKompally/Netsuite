
{#
-- Description: Incremental Load Script for Silver Layer - employees Table
-- Script Name: silver_employees.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the employees table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='employee_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'employees') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('employee_id') }}                  AS employee_id,

    -- FOREIGN KEYS
    {{ safe_integer('class_id') }}                     AS class_id,
    {{ safe_integer('def_expense_report_currency_id') }} AS currency,
    {{ safe_integer('department_id') }}             AS department_id,
    {{ safe_integer('location_id') }}               AS location_id,
    {{ safe_integer('employee_type_id') }}          AS employee_type_id,
    {{ safe_integer('subsidiary_id') }}            AS subsidiary,
    {{ clean_string('ENTITYId') }}              AS entity_id,  

    -- DETAILS
    {{ clean_string('accountnumber') }}           AS account_number,
    {{ clean_string('email') }}                     AS email,
    {{ safe_boolean('isinactive') }}             AS is_inactive,
    {{ clean_string('job_description') }}         AS job_description,
    {{ clean_string('status') }}                  AS employee_status_id,
    {{ clean_string('title') }}                    AS title,

    -- DATES / TIMESTAMPS
    {{ safe_date('create_date')}}            AS date_created,
    {{ safe_date('last_modified_date')}}        AS last_modified_date,

    -- AUDIT / METADATA
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ   AS silver_load_date

FROM raw
)

SELECT *
FROM cleaned


