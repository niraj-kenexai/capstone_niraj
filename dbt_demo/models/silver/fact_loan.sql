{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        loan_id,
        loan_type,
        loan_amount,
        interest_rate,
        tenure_months,
        loan_status,
        customer_id,
        branch_id
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE loan_id IS NOT NULL AND TRIM(loan_id) <> ''

),

cleaned AS (

    SELECT
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(loan_id)), 2) AS loan_key,
        TRY_CAST(TRIM(loan_id) AS VARCHAR(50)) AS loan_id,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(customer_id)), 2) AS customer_key,
        TRY_CAST(TRIM(customer_id) AS VARCHAR(50)) AS customer_id,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(branch_id)), 2) AS branch_key,
        TRY_CAST(TRIM(branch_id) AS VARCHAR(25)) AS branch_id,
        TRY_CAST(NULLIF(TRIM(loan_type), '') AS VARCHAR(50)) AS loan_type,
        TRY_CAST(NULLIF(TRIM(loan_amount), '') AS DECIMAL(18,2)) AS loan_amount,
        TRY_CAST(NULLIF(TRIM(interest_rate), '') AS DECIMAL(6,2)) AS interest_rate,
        TRY_CAST(ROUND(TRY_CAST(NULLIF(TRIM(tenure_months), '') AS FLOAT), 0) AS INT) AS tenure_months,
        TRY_CAST(NULLIF(TRIM(loan_status), '') AS VARCHAR(25)) AS loan_status
    FROM source_data

),

deduplicated AS (

    SELECT
        loan_key,
        loan_id,
        customer_key,
        customer_id,
        branch_key,
        branch_id,
        loan_type,
        loan_amount,
        interest_rate,
        tenure_months,
        loan_status,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id
            ORDER BY loan_amount DESC
        ) AS row_num
    FROM cleaned

)

SELECT
    loan_key,
    loan_id,
    customer_key,
    customer_id,
    branch_key,
    branch_id,
    loan_type,
    loan_amount,
    interest_rate,
    tenure_months,
    loan_status
FROM deduplicated
WHERE row_num = 1
