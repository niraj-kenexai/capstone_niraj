{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        customer_id,
        customer_name,
        customer_dob,
        customer_gender,
        customer_email,
        customer_phone,
        customer_city,
        customer_country,
        customer_segment,
        kyc_status,
        customer_annual_income,
        credit_score,
        transaction_date

    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE customer_id IS NOT NULL 
      AND TRIM(customer_id) <> ''

),

cleaned AS (

    SELECT
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(customer_id)), 2) AS customer_key,
        TRY_CAST(TRIM(customer_id) AS VARCHAR(50)) AS customer_id,
        TRY_CAST(NULLIF(TRIM(customer_name), '') AS VARCHAR(100)) AS customer_name,
        COALESCE(
            TRY_CONVERT(DATE, TRIM(customer_dob), 120),
            TRY_CONVERT(DATE, TRIM(customer_dob), 101),
            TRY_CONVERT(DATE, TRIM(customer_dob), 103),
            TRY_CONVERT(DATE, TRIM(customer_dob), 105),
            TRY_CAST(TRIM(customer_dob) AS DATE)
        ) AS customer_dob,
        TRY_CAST(NULLIF(TRIM(customer_gender), '') AS VARCHAR(10)) AS customer_gender,
        TRY_CAST(NULLIF(TRIM(customer_email), '') AS VARCHAR(100)) AS customer_email,
        TRY_CAST(REPLACE(TRIM(customer_phone), '.0', '') AS VARCHAR(25)) AS customer_phone,
        TRY_CAST(NULLIF(TRIM(customer_city), '') AS VARCHAR(50)) AS customer_city,
        TRY_CAST(NULLIF(TRIM(customer_country), '') AS VARCHAR(50)) AS customer_country,
        TRY_CAST(NULLIF(TRIM(customer_segment), '') AS VARCHAR(50)) AS customer_segment,
        TRY_CAST(NULLIF(TRIM(kyc_status), '') AS VARCHAR(25)) AS kyc_status,
        TRY_CAST(NULLIF(TRIM(customer_annual_income), '') AS DECIMAL(18,2)) AS customer_annual_income,
        TRY_CAST(NULLIF(TRIM(credit_score), '') AS INT) AS credit_score,
        COALESCE(
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 120),
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 101),
            TRY_CONVERT(DATETIME2, TRIM(transaction_date), 103),
            TRY_CAST(TRIM(transaction_date) AS DATETIME2)
        ) AS txn_timestamp

    FROM source_data

),

deduplicated AS (

    SELECT
        customer_key,
        customer_id,
        customer_name,
        customer_dob,
        customer_gender,
        customer_email,
        customer_phone,
        customer_city,
        customer_country,
        customer_segment,
        kyc_status,
        customer_annual_income,
        credit_score,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY txn_timestamp DESC, customer_annual_income DESC
        ) AS row_num
    FROM cleaned

)

SELECT
    customer_key,
    customer_id,
    customer_name,
    customer_dob,
    customer_gender,
    customer_email,
    customer_phone,
    customer_city,
    customer_country,
    customer_segment,
    kyc_status,
    customer_annual_income,
    credit_score
FROM deduplicated
WHERE row_num = 1
