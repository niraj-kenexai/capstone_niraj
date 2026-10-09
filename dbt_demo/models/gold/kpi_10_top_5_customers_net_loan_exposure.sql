{{ config(materialized='view', schema='gold') }}

WITH repayments AS (

    SELECT
        customer_key,
        SUM(amount) AS total_repayments
    FROM {{ ref('fact_transaction') }}
    WHERE transaction_type = 'LOAN_EMI_PAYMENT' 
      AND transaction_status = 'SUCCESS'
    GROUP BY customer_key

),

loan_exposures AS (

    SELECT
        b.branch_name,
        c.customer_id,
        c.customer_name,
        l.loan_type,
        l.loan_amount,
        COALESCE(r.total_repayments, 0.00) AS total_repayments,
        ROUND(l.loan_amount - COALESCE(r.total_repayments, 0.00), 2) AS net_loan_exposure
    FROM {{ ref('fact_loan') }} l
    JOIN {{ ref('dim_customer') }} c 
      ON l.customer_key = c.customer_key
    JOIN {{ ref('dim_branch') }} b 
      ON l.branch_key = b.branch_key
    LEFT JOIN repayments r 
      ON l.customer_key = r.customer_key
    WHERE l.loan_status IN ('Active', 'Open', 'Restructured')

),

ranked AS (

    SELECT
        branch_name,
        customer_id,
        customer_name,
        loan_type,
        loan_amount,
        total_repayments,
        net_loan_exposure,
        DENSE_RANK() OVER (
            PARTITION BY branch_name 
            ORDER BY net_loan_exposure DESC
        ) AS rank_in_branch
    FROM loan_exposures

)

SELECT
    branch_name,
    customer_id,
    customer_name,
    loan_type,
    loan_amount,
    total_repayments,
    net_loan_exposure,
    rank_in_branch
FROM ranked
WHERE rank_in_branch <= 5
