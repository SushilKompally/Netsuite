
{#
-- Description: Incremental Load Script for Silver Layer - transaction table
-- Script Name: transaction.sql
-- Created on: 24-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the NetSuite transaction accounting line table.
--     Standardizes types, applies cleanup macros, and enforces unique key on TRANSACTION_ID.
-- Data source version: v62.0
-- Change History:
--     24-Dec-2025 - Initial creation - Sushil Komp--     24-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='transaction_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'transactions') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (



SELECT
    -- PRIMARY KEY
    {{ safe_integer('transaction_id') }}                    AS transaction_id,

    -- FOREIGN KEYS / REFERENCES
    {{ safe_integer('accounting_period_id') }}              AS posting_period_id,
    {{ safe_integer('company_status_id') }}                 AS transaction_status_id,
    {{ safe_integer('created_by_id') }}                     AS employee_id,
    {{ safe_integer('created_from_id') }}                   AS source_transaction,
    {{ safe_integer('currency_id') }}                       AS currency_id,
    {{ safe_integer('ENTITY_id') }}                         AS internal_entity_id,
    {{ safe_integer('location_id') }}                       AS location_id,
    {{ safe_integer('partner_id') }}                        AS partner_id,
    {{ safe_integer('payment_terms_id') }}                  AS payment_method_id,
    {{ safe_integer('transaction_website') }}               AS transaction_session_vin,

    -- DETAILS (strings cleaned)
    {{ clean_string('billaddress') }}                       AS billing_address_id,
    {{ clean_string('email') }}                             AS session_shop_email,
    {{ clean_string('memo') }}                              AS memo,
    {{ clean_string('shipaddress') }}                       AS shipping_address_id,
    {{ clean_string('status') }}                            AS billing_status,
    {{ clean_string('title') }}                             AS title,
    {{ clean_string('tranid') }}                            AS tran_id,
    {{ clean_string('transaction_number') }}                AS transaction_number,
    {{ clean_string('transaction_partner') }}               AS transaction_session_ro,
    {{ clean_string('transaction_SOURCE') }}                AS source,
    {{ clean_string('transaction_type') }}                  AS transaction_type,
    {{ clean_string('RECORD_TYPE') }}                       AS record_type,

    -- NUMBERS
    {{ safe_integer('amount_unbilled') }}                   AS amount_unbilled,
    {{ safe_integer('exchange_rate') }}                     AS exchange_rate,

    -- DATES / TIMESTAMPS (Snowflake-safe)
    
    {{ safe_date('closed') }}           AS close_date,
    {{ safe_date('create_date') }}      AS created_date,
    {{ safe_date('date_last_modified') }} AS last_modified_date,
    {{ safe_date('due_date') }}         AS due_date,
    {{ safe_date('end_date') }}         AS end_date,
    {{ safe_date('start_date') }}       AS start_date,
    {{ safe_date('trandate') }}         AS tran_date,

    -- AUDIT / METADATA
    current_timestamp()::timestamp_ntz         AS silver_load_date

    FROM raw
)

SELECT *
from cleaned


