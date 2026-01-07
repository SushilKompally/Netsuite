
{{ 
  config(
    incremental_strategy='merge',
    unique_key='TRANSACTIONS_UNIQUE_ID',
  ) 
}}

WITH source_rows AS (
    
    SELECT
        -- PRIMARY KEY
        {{ dbt_utils.surrogate_key(['tl.transaction_id','tl.transaction_line_id']) }} AS transactions_unique_id,

        -- FOREIGN KEYS
        tl.transaction_id,
        t.tran_id,
        tl.transaction_line_id,
        tal.account_id,
        tl.item_id,
        tl.class_id,
        t.posting_period_id,
        t.employee_id,
        tl.entity_id,
        t.billing_address_id,
        t.shipping_address_id,
        t.currency_id,
        tl.subsidiary_id,
        t.location_id,
        t.transaction_status_id,
        tl.department_id,
        cer.accounting_book_id,

        -- DETAILS
        a.account_number,
        a.account_type,
        a.account_name,
        t.transaction_type,
        t.transaction_number,
        t.title,
        tl.transaction_line_type,
        tl.item_type,
        tl.accounting_line_type,
        tl.quantity,
        t.memo,
        t.billing_status,
        t.transaction_session_ro,
        tal.transaction_accounting_posting_flag,

        -- MEASURES
        tal.net_amount,
        tal.amount,
        ROUND(tal.net_amount * t.exchange_rate, 2) AS converted_net_amount,
        ROUND(tal.net_amount * TRY_CAST(tl.quantity AS NUMBER), 2) AS bom_quantity,

        -- DERIVED / HELPER KEYS
        {{ dbt_utils.surrogate_key(['tal.account_id','tl.class_id']) }} AS chart_of_accounts_unique_id,
        {{ dbt_utils.surrogate_key(['tl.subsidiary_id','t.posting_period_id','t.currency_id']) }} AS consolidated_exchange_rate_unique_id,

        -- DATES / TIMESTAMPS
        t.tran_date,
        t.start_date,
        t.end_date,
        t.due_date,
        t.close_date,
        per.start_date AS posting_period_date,

        -- LOOKUPS
        txs.transaction_status_name,

        -- AUDIT / METADATA
        t.record_type,
        t.last_modified_date

    FROM {{ ref('transaction_accounting_line') }} tal
    LEFT JOIN {{ ref('transaction_lines') }} tl
    ON tl.transaction_line_id = tal.transaction_line_id
    LEFT JOIN {{ ref('transaction') }} t
    ON t.transaction_id = tl.transaction_id
    LEFT JOIN {{ ref('accounts') }} a 
    ON a.account_id = tal.account_id
    LEFT JOIN {{ ref('accounting_period') }} per 
    ON per.posting_period_id = t.posting_period_id
    LEFT JOIN {{ ref('transaction_status') }} txs 
    ON txs.transaction_status_id = t.transaction_status_id 
    LEFT JOIN {{ ref('consolidated_exchange_rates') }} cer 
    ON cer.posting_period_id = t.posting_period_id  
    AND cer.to_subsidiary_id = tl.subsidiary_id
    AND cer.from_currency_id = t.currency_id

    {% if is_incremental() %}
    -- Only process changes since the max last_modified_date in target
    WHERE CAST(t.last_modified_date AS TIMESTAMP_NTZ) > (
        SELECT COALESCE(MAX(last_modified_date), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
    
)

-- dbt uses this SELECT as the "USING" subquery for MERGE

SELECT
    -- PRIMARY KEY
    transactions_unique_id,

    -- FOREIGN KEYS
    transaction_id,
    tran_id,
    transaction_line_id,
    account_id,
    item_id,
    class_id,
    posting_period_id,
    employee_id,
    entity_id,
    billing_address_id,
    shipping_address_id,
    currency_id,
    subsidiary_id,
    location_id,
    transaction_status_id,
    department_id,
    accounting_book_id,

    -- DETAILS
    account_number,
    account_type,
    account_name,
    transaction_type,
    transaction_number,
    title,
    transaction_line_type,
    item_type,
    accounting_line_type,
    quantity,
    memo,
    billing_status,
    transaction_session_ro,
    transaction_accounting_posting_flag,

    -- MEASURES
    net_amount,
    amount,
    converted_net_amount,
    bom_quantity,

    -- DERIVED / HELPER KEYS
    chart_of_accounts_unique_id,
    consolidated_exchange_rate_unique_id,

    -- DATES / TIMESTAMPS
    tran_date,
    start_date,
    end_date,
    due_date,
    close_date,
    posting_period_date,

    -- LOOKUPS
    transaction_status_name,

    -- AUDIT / METADATA
    record_type,
    last_modified_date
FROM source_rows
