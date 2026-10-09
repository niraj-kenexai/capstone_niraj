{{ config(materialized='view', schema='gold') }}

SELECT
    transaction_type,
    ROUND(AVG(amount), 2) AS average_transaction_amount
FROM {{ ref('fact_transaction') }}
WHERE transaction_type IS NOT NULL
GROUP BY transaction_type
