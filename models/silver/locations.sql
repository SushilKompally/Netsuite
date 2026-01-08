
{#
-- Description: Incremental Load Script for Silver Layer - locations Table
-- Script Name: silver_locations.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the locations table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='location_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'locations') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('id') }}               AS location_id,

    -- FOREIGN KEYS
    {{ safe_integer('parent') }}           AS parent,
    {{ safe_integer('SUBSIDIARY') }}       AS subsidiary_id,
    {{ safe_integer('locationtype') }}     AS location_type,

    -- DETAILS
    {{ clean_string('fullname') }}         AS location_full_name,
    {{ clean_string('name') }}             AS location_name,
    {{ safe_boolean('isinactive') }}       AS is_inactive,
    {{ clean_string('latitude') }}         AS latitude,    
    {{ clean_string('longitude') }}        AS longitude,    

    -- DATES / TIMESTAMPS
    {{ safe_date('lastmodifieddate') }}    AS last_modified_date,

    -- AUDIT / METADATA
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ     AS silver_load_date

FROM raw
)

SELECT *
FROM cleaned


