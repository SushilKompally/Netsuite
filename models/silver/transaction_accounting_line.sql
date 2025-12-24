
/*
-- Description: Incremental Load Script for Silver Layer - transaction_accounting_line table
-- Script Name: transaction_accounting_line.sql
-- Created on: 24-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the NetSuite transaction accounting line table.
--     Standardizes types, applies cleanup macros, and enforces unique key on TRANSACTION_LINE_ID.
-- Data source version: v62.0
-- Change History:
--     24-Dec-2025 - Initial creation - Sushil Komp--     24-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='transaction_line_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'transaction_accounting_line') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

    SELECT
        -- PRIMARY KEY
        {{ safe_integer('transactionLine') }}      AS transaction_line_id,

        -- FOREIGN KEYS / REFERENCES
        {{ safe_integer('account') }}              AS account_id,
        {{ safe_integer('transaction') }}          AS transaction_id,
        {{ safe_integer('accountingBook') }}       AS accounting_book_id,

        -- DETAILS
        {{ clean_string('accountType') }}          AS account_type,
        {{ safe_integer('amount') }}               AS amount,
        {{ safe_integer('amountPaid') }}           AS amount_Paid,
        {{ safe_integer('amountUnpaid') }}         AS amount_Un_paid,
        {{ safe_integer('exchangeRate') }}         AS exchange_rate,
        {{ safe_integer('netAmount') }}            AS net_Amount,
        posting                                   AS transaction_accounting_posting_flag,

        -- DATES / TIMESTAMPS
      lastModifiedDate  AS LAST_MODIFIED_DATE,

        -- AUDIT / METADATA
        current_timestamp()::timestamp_ntz         AS SILVER_LOAD_DATE

    FROM raw
)

SELECT *
from cleaned