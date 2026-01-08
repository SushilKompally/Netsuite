
{#
-- Description: Incremental Load Script for Silver Layer - currencyrates Table
-- Script Name: silver_currencyrates.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the currencyrates table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='currency_rate_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'currencyrates') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('currencyrate_id') }}     AS currency_rate_id,

    -- DATES
    date_effective         AS effective_date,
    date_last_modified    AS last_modified_date,

    -- DETAILS
    {{ safe_integer('exchange_rate') }}       AS exchange_rate,        
    {{ clean_string('externalId') }}          AS externalid,
    {{ safe_integer('transactionCurrency') }} AS transaction_currency,

    -- AUDIT
    CURRENT_TIMESTAMP()                       AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned

