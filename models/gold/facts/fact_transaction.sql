
{{ 
  config(
    incremental_strategy='merge',
    unique_key='TRANSACTIONS_UNIQUE_ID',
  ) 
}}

WITH source_rows AS (

    SELECT
        -- PRIMARY KEY
        {{ dbt_utils.surrogate_key(['tl.TRANSACTION_ID','tl.TRANSACTION_LINE_ID']) }} AS TRANSACTIONS_UNIQUE_ID,

        -- FOREIGN KEYS
        tl.TRANSACTION_ID,
        t.TRAN_ID,
        tl.TRANSACTION_LINE_ID,
        tal.ACCOUNT_ID,
        tl.ITEM_ID,
        tl.CLASS_ID,
        t.POSTING_PERIOD_ID,
        t.EMPLOYEE_ID,
        tl.ENTITY_ID,
        t.BILLING_ADDRESS_ID,
        t.SHIPPING_ADDRESS_ID,
        t.CURRENCY_ID,
        tl.SUBSIDIARY_ID,
        t.LOCATION_ID,
        t.TRANSACTION_STATUS_ID,
        tl.DEPARTMENT_ID,
        cer.ACCOUNTING_BOOK_ID,

        -- DETAILS
        a.ACCOUNT_NUMBER,
        a.ACCOUNT_TYPE,
        a.ACCOUNT_NAME,
        t.TRANSACTION_TYPE,
        t.TRANSACTION_NUMBER,
        t.TITLE,
        tl.TRANSACTION_LINE_TYPE,
        tl.ITEM_TYPE,
        tl.ACCOUNTING_LINE_TYPE,
        tl.QUANTITY,
        t.MEMO,
        t.BILLING_STATUS,
        t.TRANSACTION_SESSION_RO,
        tal.TRANSACTION_ACCOUNTING_POSTING_FLAG,

        -- MEASURES
        tal.NET_AMOUNT,
        tal.AMOUNT,
        ROUND(tal.NET_AMOUNT * t.EXCHANGE_RATE, 2) AS CONVERTED_NET_AMOUNT,
        ROUND(tal.NET_AMOUNT * TRY_CAST(tl.QUANTITY AS NUMBER), 2) AS BOM_QUANTITY,

        -- DERIVED / HELPER KEYS
        {{ dbt_utils.surrogate_key(['tal.ACCOUNT_ID','tl.CLASS_ID']) }} AS CHART_OF_ACCOUNTS_UNIQUE_ID,
        {{ dbt_utils.surrogate_key(['tl.SUBSIDIARY_ID','t.POSTING_PERIOD_ID','t.CURRENCY_ID']) }} AS CONSOLIDATED_EXCHANGE_RATE_UNIQUE_ID,

        -- DATES / TIMESTAMPS
        t.TRAN_DATE,
        t.START_DATE,
        t.END_DATE,
        t.DUE_DATE,
        t.CLOSE_DATE,
        per.START_DATE AS POSTING_PERIOD_DATE,

        -- LOOKUPS
        txs.TRANSACTION_STATUS_NAME,

        -- AUDIT / METADATA
        t.RECORD_TYPE,
        t.LAST_MODIFIED_DATE
               

    FROM {{ ref('transaction_accounting_line') }} tal
    LEFT JOIN {{ ref('transaction_lines') }} tl
      ON tl.TRANSACTION_LINE_ID = tal.TRANSACTION_LINE_ID
    LEFT JOIN {{ ref('transaction') }} t
      ON t.TRANSACTION_ID = tl.TRANSACTION_ID
    LEFT JOIN {{ ref('accounts') }} a 
      ON a.ACCOUNT_ID = tal.ACCOUNT_ID
    LEFT JOIN {{ ref('accounting_period') }} per 
      ON per.POSTING_PERIOD_ID = t.POSTING_PERIOD_ID
    LEFT JOIN {{ ref('transaction_status') }} txs 
      ON txs.TRANSACTION_STATUS_ID = t.TRANSACTION_STATUS_ID 
    LEFT JOIN {{ ref('consolidated_exchange_rates') }} cer 
      ON cer.POSTING_PERIOD_ID = t.POSTING_PERIOD_ID  
     AND cer.TO_SUBSIDIARY_ID = tl.SUBSIDIARY_ID
     AND cer.FROM_CURRENCY_ID = t.CURRENCY_ID
 

    {% if is_incremental() %}
      -- Only process changes since the max LAST_MODIFIED_DATE in target
      WHERE CAST(t.LAST_MODIFIED_DATE AS TIMESTAMP_NTZ) > (
        SELECT COALESCE(MAX(LAST_MODIFIED_DATE), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
      )
    {% endif %}
)

-- dbt uses this SELECT as the "USING" subquery for MERGE
SELECT
    -- PRIMARY KEY
    TRANSACTIONS_UNIQUE_ID,

    -- FOREIGN KEYS
    TRANSACTION_ID,
    TRAN_ID,
    TRANSACTION_LINE_ID,
    ACCOUNT_ID,
    ITEM_ID,
    CLASS_ID,
    POSTING_PERIOD_ID,
    EMPLOYEE_ID,
    ENTITY_ID,
    BILLING_ADDRESS_ID,
    SHIPPING_ADDRESS_ID,
    CURRENCY_ID,
    SUBSIDIARY_ID,
    LOCATION_ID,
    TRANSACTION_STATUS_ID,
    DEPARTMENT_ID,
    ACCOUNTING_BOOK_ID,

    -- DETAILS
    ACCOUNT_NUMBER,
    ACCOUNT_TYPE,
    ACCOUNT_NAME,
    TRANSACTION_TYPE,
    TRANSACTION_NUMBER,
    TITLE,
    TRANSACTION_LINE_TYPE,
    ITEM_TYPE,
    ACCOUNTING_LINE_TYPE,
    QUANTITY,
    MEMO,
    BILLING_STATUS,
    TRANSACTION_SESSION_RO,
    TRANSACTION_ACCOUNTING_POSTING_FLAG,

    -- MEASURES
    NET_AMOUNT,
    AMOUNT,
    CONVERTED_NET_AMOUNT,
    BOM_QUANTITY,

    -- DERIVED / HELPER KEYS
    CHART_OF_ACCOUNTS_UNIQUE_ID,
    CONSOLIDATED_EXCHANGE_RATE_UNIQUE_ID,

    -- DATES / TIMESTAMPS
    TRAN_DATE,
    START_DATE,
    END_DATE,
    DUE_DATE,
    CLOSE_DATE,
    POSTING_PERIOD_DATE,

    -- LOOKUPS
    TRANSACTION_STATUS_NAME,

    -- AUDIT / METADATA
    RECORD_TYPE,
    LAST_MODIFIED_DATE
FROM source_rows
