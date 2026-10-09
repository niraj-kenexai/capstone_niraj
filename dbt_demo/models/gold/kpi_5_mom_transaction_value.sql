{{ config(materialized='view', schema='gold') }}

WITH monthly_totals AS (

    SELECT
        transaction_month,
        ROUND(SUM(amount), 2) AS total_transaction_value
    FROM {{ ref('fact_transaction') }}
    WHERE transaction_month IS NOT NULL
    GROUP BY transaction_month

),

with_lag AS (

    SELECT
        transaction_month,
        total_transaction_value,
        LAG(total_transaction_value) OVER (ORDER BY transaction_month) AS previous_month_value
    FROM monthly_totals

)

SELECT
    transaction_month,
    total_transaction_value,
    previous_month_value,
    ROUND(
        CASE 
            WHEN previous_month_value IS NULL OR previous_month_value = 0 THEN NULL
            ELSE ((total_transaction_value - previous_month_value) * 100.0) / previous_month_value
        END, 
        2
    ) AS mom_growth_percentage
FROM with_lag
