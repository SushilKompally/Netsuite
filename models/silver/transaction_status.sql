
{#
-- Description: Incremental Load Script for Silver Layer - transaction_status table
-- Script Name: transaction_status.sql
-- Created on: 24-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the NetSuite transaction accounting line table.
--     Standardizes types, applies cleanup macros, and enforces unique key on TRANSACTION_status_ID.
-- Data source version: v62.0
-- Change History:
--     24-Dec-2025 - Initial creation - Sushil Komp--     24-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='transaction_status_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'transaction_status') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (


 SELECT
        -- PRIMARY KEY
        {{ safe_integer('TRANSACTION_STATUS_ID') }}     AS transaction_status_id,

        -- DETAILS
        {{ clean_string('TRANSACTION_STATUS_FULL_NAME') }} AS transaction_status_full_name,
        {{ clean_string('TRANSACTION_STATUS_NAME') }}      AS transaction_status_name,
        {{ safe_integer('TRAN_CUSTOM_TYPE_ID') }}          AS tran_custom_type_id,
        {{ clean_string('TRANSACTION_TYPE') }}             AS transaction_type,

        -- TIMESTAMPS / METADATA
        {{ safe_date('INGESTION_TIME') }}       AS ingestion_time,
        current_timestamp()::timestamp_ntz         AS silver_load_date

    FROM raw
)

SELECT *
from cleaned






