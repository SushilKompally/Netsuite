
{#
-- Description: Incremental Load Script for Silver Layer - currencies Table
-- Script Name: silver_currencies.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the currencies table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='currency_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'currencies') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- KEYS
    {{ safe_integer('currency_id') }}         AS currency_id,

    -- DATES
    {{safe_date('date_last_modified')}}     AS last_modified_date,

    -- DETAILS / FLAGS
    {{ safe_integer('exchangeRate') }}        AS exchange_rate,         -- INT as requested
    {{ safe_boolean('is_inactive') }}         AS is_inactive,
    {{ safe_boolean('isBaseCurrency') }}      AS is_base_currency,
    {{ clean_string('name') }}                AS currency_name,
    {{ clean_string('symbol') }}              AS display_symbol,

    -- AUDIT
    CURRENT_TIMESTAMP()                       AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned
