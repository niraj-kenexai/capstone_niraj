{{ config(materialized='view', schema='gold') }}

SELECT
    loan_type,
    COUNT(loan_id) AS loan_count,
    ROUND(SUM(loan_amount), 2) AS total_loan_exposure,
    ROUND(AVG(interest_rate), 2) AS average_interest_rate,
    ROUND(AVG(CAST(tenure_months AS FLOAT)), 1) AS average_tenure_months,
    ROUND(SUM(loan_amount * (interest_rate / 100.0) * (tenure_months / 12.0)), 2) AS estimated_interest
FROM {{ ref('fact_loan') }}
WHERE loan_type IS NOT NULL
GROUP BY loan_type
