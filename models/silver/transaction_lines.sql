
/*
-- Description: Incremental Load Script for Silver Layer - transaction_line table
-- Script Name: transaction_line.sql
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
    FROM {{ source('netsuite_bronze', 'transaction_lines') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (


SELECT
    -- PRIMARY KEY
    {{ safe_integer('id') }}                         AS transaction_line_id,

    -- DETAILS / KEYS
    {{ clean_string('accountinglinetype') }}         AS accounting_line_type,
    {{ clean_string('transactionlinetype') }}         AS transaction_line_type,
    actualshipdate           AS actual_ship_date,
    billeddate              AS billed_date,
    {{ safe_integer('billingschedule') }}            AS billing_schedule_id,
    {{ safe_integer('class') }}                      AS class_id,
    closedate                AS close_date,
    {{ safe_integer('createdfrom') }}                AS created_from_transaction_id,
    {{ safe_integer('department') }}                 AS department_id,
    {{ safe_integer('ENTITY') }}                     AS entity_id,
    {{ safe_integer('expenseaccount') }}             AS revenue_account_name,       
    {{ safe_integer('ITEM') }}                       AS item_id,
    {{ clean_string('ITEMtype') }}                   AS item_type,
    linelastmodifieddate     AS line_lastmodified_date,
    {{ safe_integer('linesequencenumber') }}         AS line_sequence_number_id,
    {{ safe_integer('location') }}                   AS location_id,
    {{ safe_integer('paymentmethod') }}              AS payment_method_id,
    {{ safe_integer('price') }}                      AS price_id,
    {{ safe_integer('revenueelement') }}             AS zab_revenue_detail_id,
    {{ safe_integer('SUBSIDIARY') }}                 AS subsidiary_id,
    {{ safe_integer('transaction') }}                AS transaction_id,
    {{ safe_integer('uniquekey') }}                  AS unique_key,
    {{ safe_integer('units') }}                      AS unit_id,

    -- AMOUNTS / METRICS
    {{ safe_integer('foreignamount') }}               AS foreign_amount,
    {{ safe_integer('netamount') }}                   AS net_amount,
    {{ clean_string('orderpriority') }}              AS order_priority,
    {{ safe_integer('quantity') }}                    AS quantity,
    {{ safe_integer('rate') }}                        AS rate,

    -- FLAGS
    {{ safe_boolean('isbillable') }}                 AS is_billable,
    {{ safe_boolean('isclosed') }}                   AS is_closed,
    {{ safe_boolean('iscogs') }}                     AS is_cogs,
    {{ safe_boolean('isfullyshipped') }}             AS is_fully_shipped,
    {{ safe_boolean('taxline') }}                    AS tax_line,
    {{ safe_boolean('transactiondiscount') }}        AS transaction_discount,

    -- FREE TEXT
    {{ clean_string('memo') }}                       AS memo,


    -- AUDIT / METADATA
    current_timestamp()::timestamp_ntz         AS silver_load_date

    FROM raw
)

SELECT *
from cleaned






