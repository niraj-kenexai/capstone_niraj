{{ config(materialized='view', schema='gold') }}

SELECT
    payment_channel,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    ROUND(
        (SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1.0 ELSE 0.0 END) * 100.0) / 
        NULLIF(COUNT(transaction_id), 0), 
        2
    ) AS success_rate
FROM {{ ref('fact_transaction') }}
WHERE payment_channel IS NOT NULL
GROUP BY payment_channel
