{{ config(materialized='view', schema='gold') }}

SELECT
    transaction_month,
    transaction_type,
    COUNT(transaction_id) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_transaction_amount,
    ROUND(AVG(amount), 2) AS average_transaction_amount,
    SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transaction_count,
    SUM(CASE WHEN transaction_status IN ('FAILED', 'DECLINED') THEN 1 ELSE 0 END) AS failed_transaction_count,
    SUM(CASE WHEN is_flagged_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transaction_count,
    ROUND(SUM(CASE WHEN is_flagged_fraud = 1 THEN amount ELSE 0 END), 2) AS fraud_amount,
    ROUND(
        (SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1.0 ELSE 0.0 END) * 100.0) /
        NULLIF(COUNT(transaction_id), 0),
        2
    ) AS success_rate
FROM {{ ref('fact_transaction') }}
WHERE transaction_month IS NOT NULL AND transaction_type IS NOT NULL
GROUP BY transaction_month, transaction_type
