
{#
-- Description: Incremental Load Script for Silver Layer - departments Table
-- Script Name: silver_departments.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the departments table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='department_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'departments') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
   
    -- KEYS
    {{ safe_integer('department_id') }}     AS department_id,
    {{ safe_integer('parent_id') }}         AS parent,

    -- DETAILS
    {{ clean_string('full_name') }}         AS department_full_name,
    {{ safe_boolean('isinactive') }}        AS is_inactive,
    {{ clean_string('name') }}              AS department_name,

    -- DATES
    {{safe_date(date_last_modified )}}  AS last_modified_date,

    -- AUDIT
    CURRENT_TIMESTAMP()                     AS silver_load_date

FROM raw

)

SELECT *
FROM cleaned
