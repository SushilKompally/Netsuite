
/*
-- Description: Incremental Load Script for Silver Layer - entity Table
-- Script Name: silver_entity.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the entity table in the NetSuite data pipeline.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='internal_entity_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'entity') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('id') }}                 AS internal_entity_id,

    -- FOREIGN KEYS
    {{ safe_integer('contact') }}            AS contact_id,
    {{ safe_integer('customer') }}           AS customer_id,
    {{ safe_integer('employee') }}           AS employee_id,
    {{ safe_integer('"group"') }}            AS group_id,
    {{ safe_integer('parent') }}             AS parent_id,
    {{ safe_integer('partner') }}            AS partner_id,
    {{ safe_integer('vendor') }}             AS vendor_id,

    -- DETAILS
    {{ clean_string('email') }}              AS email,
    {{ clean_string('ENTITYid') }}           AS entity_id,       
    {{ clean_string('ENTITYtitle') }}        AS entity_title,
    {{ clean_string('firstname') }}          AS first_name,
    {{ clean_string('lastname') }}           AS last_name,
    {{ clean_string('"type"') }}             AS entity_type,
    {{ safe_boolean('isinactive') }}         AS is_inactive,
    {{ safe_boolean('isperson') }}           AS is_person,

    -- DATES / TIMESTAMPS
    datecreated           AS date_created,
    lastmodifieddate      AS last_modified_date,

    -- AUDIT / METADATA
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ       AS silver_load_date

FROM raw
)

SELECT *
FROM cleaned
