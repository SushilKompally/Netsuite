
{#
-- Description: Incremental Load Script for Silver Layer - consolidated_exchange_rates Table
-- Script Name: silver_consolidated_exchange_rates.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the consolidated_exchange_rates table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='consolidated_exchange_rate_id',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'consolidated_exchange_rates') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ clean_string('consolidated_exchange_rate_id') }} AS consolidated_exchange_rate_id,

    -- FOREIGN KEYS
    {{ safe_integer('accounting_book_id') }}  AS accounting_book_id,
    {{ safe_integer('accounting_period_id') }} AS posting_period_id,
    {{ safe_integer('from_subsidiary_id') }}  AS from_subsidiary_id,
    {{ safe_integer('to_subsidiary_id') }}    AS to_subsidiary_id,
    {{ safe_integer('fromCurrency') }}        AS from_currency_id,
    {{ safe_integer('toCurrency') }}          AS to_currency_id,

    -- DETAILS 
    {{ safe_integer('average_rate') }}        AS average_rate,
    {{ safe_integer('current_rate') }}        AS current_rate,
    {{ safe_integer('historical_rate') }}     AS historical_rate,

    -- AUDIT
    CURRENT_TIMESTAMP()                       AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned
