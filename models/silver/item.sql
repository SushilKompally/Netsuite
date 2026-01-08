
{#
-- Description: Incremental Load Script for Silver Layer - item Table
-- Script Name: silver_items.sql
-- Created on: 23-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for NetSuite items.
-- Data source version: v62.0
-- Change History:
--     23-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='item_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('netsuite_bronze', 'item') }}
    WHERE 1=1
    {{ incremental_filter() }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    {{ safe_integer('id') }}                     AS item_id,

    -- FOREIGN KEYS
    {{ safe_integer('class') }}                  AS class_id,
    {{ safe_integer('department') }}             AS department_id,
    {{ safe_integer('incomeaccount') }}          AS income_account,
    {{ safe_integer('location') }}               AS location_id,
    {{ safe_integer('parent') }}                 AS parent_id,
    {{ safe_integer('pricinggroup') }}           AS pricing_group,
    {{ safe_integer('saleunit') }}               AS sale_unit,
    {{ safe_integer('stockunit') }}              AS stock_unit,
    {{ safe_integer('SUBSIDIARY') }}             AS subsidiary_id,
    {{ safe_integer('unitstype') }}              AS units_type,

    -- DETAILS
    {{ safe_decimal('averagecost', 18, 6) }}     AS average_cost,
    {{ safe_decimal('cost', 18, 6) }}            AS cost,
    {{ clean_string('costingmethod') }}          AS costing_method,
    {{ clean_string('description') }}            AS description,
    {{ clean_string('displayname') }}            AS display_name,
    {{ clean_string('fullname') }}               AS item_full_name,
    {{ clean_string('ITEMid') }}                 AS item_name,
    {{ clean_string('ITEMtype') }}               AS item_type,
    {{ safe_decimal('lastpurchaseprice', 18, 6) }} AS last_purchase_price,
    {{ clean_string('manufacturer') }}           AS manufacturer,
    {{ safe_integer('maximumquantity') }}        AS maximum_quantity,
    {{ clean_string('storedescription') }}       AS store_description,
    {{ clean_string('storedetaileddescription') }} AS store_detailed_description,
    {{ clean_string('storedisplayname') }}       AS store_display_name,
    {{ clean_string('subtype') }}                AS sub_type,
    {{ safe_decimal('totalvalue', 18, 6) }}      AS total_value,
    {{ clean_string('vendorname') }}             AS vendor_name,
    {{ safe_decimal('weight', 18, 6) }}          AS weight,
    {{ safe_decimal('shippingcost', 18, 6) }}    AS shipping_cost,
    {{ safe_boolean('isfulfillable') }}          AS is_ful_fillable,
    {{ safe_boolean('isinactive') }}             AS is_inactive,
    {{ safe_integer('quantityonhand') }}         AS total_quantity_on_hand,

    -- DATES / TIMESTAMPS
    {{ safe_date('createddate') }}           AS created_date,
    {{ safe_date('lastmodifieddate') }}    AS last_modified_date,

    -- AUDIT / METADATA
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ       AS silver_load_date

FROM raw
)

SELECT *
FROM cleaned

