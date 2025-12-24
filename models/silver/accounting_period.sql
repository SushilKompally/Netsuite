
/*
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
*/

{{ config(
    unique_key='posting_period_id',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata(
        ) }}
    FROM {{ source('netsuite_bronze', 'accounting_periods') }}
    WHERE 1=1
    {{ incremental_filter()
    }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    accounting_period_id AS posting_period_id,

    -- DATES
    closed_on           AS closed_on_date,
    date_last_modified  AS last_modified_date,
    ending             AS end_date,
    starting           AS start_date,

    -- DETAILS
   {{ clean_string('full_name') }}  AS period_name,
    year_0    AS year,

    -- LOAD / AUDIT
    CURRENT_TIMESTAMP()               AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned
