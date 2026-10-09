{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        transaction_id,
        transaction_date,
        transaction_type,
        amount,
        currency,
        transaction_status,
        payment_channel,
        payment_method,
        running_balance,
        is_flagged_fraud,
        merchant_name,
        merchant_category,
        customer_id,
        account_id,
        branch_id
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''

),

parsed AS (

    SELECT
        transaction_id,
        COALESCE(
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 120),
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 101),
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 103),
            TRY_CAST(TRIM(transaction_date) AS DATETIME2)
        ) AS txn_timestamp,
        transaction_type,
        amount,
        currency,
        transaction_status,
        payment_channel,
        payment_method,
        running_balance,
        is_flagged_fraud,
        merchant_name,
        merchant_category,
        customer_id,
        account_id,
        branch_id
    FROM source_data

)

SELECT
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(transaction_id)), 2) AS transaction_key,
    TRY_CAST(TRIM(transaction_id) AS VARCHAR(50)) AS transaction_id,
    CAST(FORMAT(txn_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(customer_id)), 2) AS customer_key,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(branch_id)), 2) AS branch_key,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(account_id)), 2) AS account_key,
    txn_timestamp AS transaction_date,
    FORMAT(txn_timestamp, 'yyyy-MM') AS transaction_month,
    TRY_CAST(NULLIF(TRIM(transaction_type), '') AS VARCHAR(50)) AS transaction_type,
    TRY_CAST(NULLIF(TRIM(amount), '') AS DECIMAL(18,2)) AS amount,
    TRY_CAST(NULLIF(TRIM(currency), '') AS VARCHAR(10)) AS currency,
    TRY_CAST(NULLIF(TRIM(transaction_status), '') AS VARCHAR(25)) AS transaction_status,
    TRY_CAST(NULLIF(TRIM(payment_channel), '') AS VARCHAR(50)) AS payment_channel,
    TRY_CAST(NULLIF(TRIM(payment_method), '') AS VARCHAR(50)) AS payment_method,
    TRY_CAST(NULLIF(TRIM(running_balance), '') AS DECIMAL(18,2)) AS running_balance,
    CASE 
        WHEN LOWER(TRIM(is_flagged_fraud)) IN ('true', '1') THEN 1 
        ELSE 0 
    END AS is_flagged_fraud,
    TRY_CAST(NULLIF(TRIM(merchant_name), '') AS VARCHAR(100)) AS merchant_name,
    TRY_CAST(NULLIF(TRIM(merchant_category), '') AS VARCHAR(50)) AS merchant_category
FROM parsed
