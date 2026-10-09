{{ config(materialized='view', schema='gold') }}

SELECT
    b.branch_name,
    l.loan_type,
    COUNT(l.loan_id) AS total_subprime_loans,
    SUM(CASE WHEN l.loan_status IN ('Delinquent', 'Default', 'Charged Off') THEN 1 ELSE 0 END) AS defaulted_subprime_loans,
    ROUND(
        (SUM(CASE WHEN l.loan_status IN ('Delinquent', 'Default', 'Charged Off') THEN 1.0 ELSE 0.0 END) * 100.0) /
        NULLIF(COUNT(l.loan_id), 0),
        2
    ) AS default_rate_pct
FROM {{ ref('fact_loan') }} l
JOIN {{ ref('dim_customer') }} c 
  ON l.customer_key = c.customer_key
JOIN {{ ref('dim_branch') }} b 
  ON l.branch_key = b.branch_key
WHERE c.credit_score < 650
GROUP BY b.branch_name, l.loan_type
