
/*
-- Description: Incremental Load Script for Silver Layer - classification Table
-- Script Name: silver_classification.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the classification table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='class_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'classification') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PK
    {{ safe_integer('id') }}       AS class_id,

    
    -- DETAILS
    {{ clean_string('fullname') }} AS class_full_name,
    {{ safe_boolean('isinactive') }} AS is_inactive,
    lastmodifieddate AS last_modified_date,
    {{ clean_string('name') }}     AS class_name,
    {{ safe_integer('parent') }}   AS parent,

    -- AUDIT
    CURRENT_TIMESTAMP()            AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned
