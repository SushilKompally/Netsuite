
/*
-- Description: Incremental Load Script for Silver Layer - subsidiaries Table
-- Script Name: silver_locations.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the locations table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='subsidiary_id',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'subsidiaries') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
   -- PRIMARY KEY
   {{ safe_integer('subsidiary_id') }}              AS SUBSIDIARY_ID,

   -- DETAILS
   {{ safe_integer('base_currency_id') }}           AS CURRENCY_ID,
   {{ clean_string('full_name') }}                  AS SUBSIDIARY_FULL_NAME,
   isinactive                                       AS IS_INACTIVE,
   {{ clean_string('name') }}                       AS SUBSIDIARY_NAME,
   {{ safe_integer('parent_id') }}                  AS PARENT_ID,

   
    -- AUDIT / METADATA
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ     AS silver_load_date

FROM raw
)

SELECT *
FROM cleaned


