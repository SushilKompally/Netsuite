
{#
-- Description: Incremental Load Script for Silver Layer - accounting_period Table
-- Script Name: silver_accounting_period.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--   Incremental load from Bronze to Silver for accounting_period in the NetSuite pipeline.
-- Data source version: v62.0
-- Change History:
--   23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    materialized='incremental',
    unique_key='posting_period_id',
    incremental_strategy='merge',
    on_schema_change= 'append_new_columns'
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('netsuite_bronze', 'accounting_periods') }}
  where 1=1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- PRIMARY KEY
    {{ clean_string('accounting_period_id') }} as posting_period_id,

    -- DATES
    {{ safe_date('closed_on') }}          as closed_on_date,
    {{ safe_timestamp_ntz('date_last_modified') }} as last_modified_date,
    {{ safe_date('ending') }}             as end_date,
    {{ safe_date('starting') }}           as start_date,

    -- DETAILS
    {{ clean_string('full_name') }}        as period_name,
    {{ clean_string('year_0') }}               as year,

    -- LOAD / AUDIT
    current_timestamp()                    as silver_load_date

  from raw
)

select *
from cleaned
