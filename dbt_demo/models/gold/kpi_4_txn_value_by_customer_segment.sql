{{ config(materialized='view', schema='gold') }}

SELECT
    c.customer_segment,
    COUNT(f.transaction_id) AS transaction_count,
    ROUND(SUM(f.amount), 2) AS total_transaction_value,
    ROUND(AVG(f.amount), 2) AS average_transaction_value
FROM {{ ref('fact_transaction') }} f
JOIN {{ ref('dim_customer') }} c 
  ON f.customer_key = c.customer_key
WHERE c.customer_segment IS NOT NULL
GROUP BY c.customer_segment
