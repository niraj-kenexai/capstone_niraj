{{ config(materialized='view', schema='gold') }}

WITH branch_monthly AS (

    SELECT 
        b.branch_id,
        b.branch_name,
        f.transaction_month,
        COUNT(f.transaction_id) AS transaction_count,
        ROUND(SUM(f.amount), 2) AS total_transaction_amount
    FROM {{ ref('fact_transaction') }} f
    JOIN {{ ref('dim_branch') }} b 
      ON f.branch_key = b.branch_key
    WHERE f.transaction_month IS NOT NULL
    GROUP BY b.branch_id, b.branch_name, f.transaction_month

),

month_per AS (

    SELECT
        branch_id,
        branch_name,
        transaction_month,
        transaction_count,
        total_transaction_amount,
        LAG(total_transaction_amount) OVER (
            PARTITION BY branch_id 
            ORDER BY transaction_month
        ) AS previous_month_transaction_amount
    FROM branch_monthly

),

final AS (

    SELECT 
        branch_id,
        branch_name,
        transaction_month,
        transaction_count,
        total_transaction_amount,
        previous_month_transaction_amount,
        CASE
            WHEN previous_month_transaction_amount IS NULL OR previous_month_transaction_amount = 0
            THEN NULL
            ELSE ROUND(((total_transaction_amount - previous_month_transaction_amount) * 100.0) / previous_month_transaction_amount, 2)
        END AS month_over_month_growth_pct,
        RANK() OVER (
            PARTITION BY transaction_month 
            ORDER BY total_transaction_amount DESC
        ) AS branch_rank_by_transaction_amount
    FROM month_per

)

SELECT * FROM final
