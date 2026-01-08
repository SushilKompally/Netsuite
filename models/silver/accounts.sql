
{#
-- Description: Incremental Load Script for Silver Layer - accounting_period Table
-- Script Name: silver_accounting_period.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the accounting_period table in the netsuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='account_id',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata(
        ) }}
    FROM {{ source('netsuite_bronze', 'accounts') }}
    WHERE 1=1
    {{ incremental_filter()
    }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('account_id') }} AS account_id,

    -- FOREIGN KEYS
    {{ safe_integer('class_id') }}       AS class_id,
    {{ safe_integer('currency_id') }}    AS currency_id,
    {{ safe_integer('department_id') }}  AS department_id,
    {{ safe_integer('location_id') }}    AS location_id,
    {{ safe_integer('parent_id') }}      AS parent_id,
    {{ safe_integer('subsidiary') }}     AS subsidiary_id,

    -- DETAILS
    {{ clean_string('accountnumber') }}      AS account_number,
    {{ clean_string('description') }}        AS account_description,
    {{ clean_string('full_description') }}   AS display_name,
    {{ clean_string('full_name') }}          AS display_name_with_hierarchy,
    isinactive                                AS is_inactive,
    {{ clean_string('name') }}               AS account_name,
    {{ clean_string('type_name') }}          AS account_type,

    -- DATES / TIMESTAMPS
   {{ safe_date('date_last_modified')}}    AS last_modified_date,

    -- AUDIT
    CURRENT_TIMESTAMP()                      AS silver_load_date
FROM raw

)

SELECT *
FROM cleaned




