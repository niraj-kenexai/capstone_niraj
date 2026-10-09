{{ config(materialized='view', schema='gold') }}

WITH customer_monthly AS (

    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        f.transaction_month,
        ROUND(SUM(f.amount), 2) AS current_month_transaction_amount
    FROM {{ ref('fact_transaction') }} f
    JOIN {{ ref('dim_customer') }} c 
      ON f.customer_key = c.customer_key
    WHERE f.transaction_month IS NOT NULL
    GROUP BY c.customer_id, c.customer_name, c.customer_segment, f.transaction_month

),

monthly_performance AS (

    SELECT 
        customer_id,
        customer_name,
        customer_segment,
        transaction_month,
        current_month_transaction_amount,
        LAG(current_month_transaction_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_month
        ) AS previous_month_transaction_amount,
        LEAD(current_month_transaction_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_month
        ) AS next_month_transaction_amount
    FROM customer_monthly

),

final AS (

    SELECT 
        customer_id,
        customer_name,
        customer_segment,
        transaction_month,
        current_month_transaction_amount,
        previous_month_transaction_amount,
        next_month_transaction_amount,
        CASE
            WHEN previous_month_transaction_amount IS NULL OR previous_month_transaction_amount = 0
            THEN NULL
            ELSE ROUND(((current_month_transaction_amount - previous_month_transaction_amount) * 100.0) / previous_month_transaction_amount, 2)
        END AS month_over_month_growth_pct,
        RANK() OVER (
            PARTITION BY customer_segment, transaction_month 
            ORDER BY current_month_transaction_amount DESC
        ) AS customer_rank_within_segment
    FROM monthly_performance

)

SELECT * FROM final
