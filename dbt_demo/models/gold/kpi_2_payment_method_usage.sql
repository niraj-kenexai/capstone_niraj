{{ config(materialized='view', schema='gold') }}

SELECT
    payment_method,
    COUNT(transaction_id) AS transaction_count
FROM {{ ref('fact_transaction') }}
WHERE payment_method IS NOT NULL
GROUP BY payment_method
