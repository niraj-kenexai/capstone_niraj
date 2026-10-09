{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        account_id,
        account_type,
        account_open_date,
        account_status,
        customer_id,
        branch_id
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE account_id IS NOT NULL AND TRIM(account_id) <> ''

),

cleaned AS (

    SELECT
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(account_id)), 2) AS account_key,
        TRY_CAST(TRIM(account_id) AS VARCHAR(50)) AS account_id,
        TRY_CAST(NULLIF(TRIM(account_type), '') AS VARCHAR(50)) AS account_type,
        COALESCE(
            TRY_CONVERT(DATE, TRIM(account_open_date), 120),
            TRY_CONVERT(DATE, TRIM(account_open_date), 101),
            TRY_CONVERT(DATE, TRIM(account_open_date), 103),
            TRY_CONVERT(DATE, TRIM(account_open_date), 105),
            TRY_CAST(TRIM(account_open_date) AS DATE)
        ) AS account_open_date,
        TRY_CAST(NULLIF(TRIM(account_status), '') AS VARCHAR(25)) AS account_status,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(customer_id)), 2) AS customer_key,
        TRY_CAST(TRIM(customer_id) AS VARCHAR(50)) AS customer_id,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(branch_id)), 2) AS branch_key,
        TRY_CAST(TRIM(branch_id) AS VARCHAR(25)) AS branch_id
    FROM source_data

),

deduplicated AS (

    SELECT
        account_key,
        account_id,
        account_type,
        account_open_date,
        account_status,
        customer_key,
        customer_id,
        branch_key,
        branch_id,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY account_open_date DESC
        ) AS row_num
    FROM cleaned

)

SELECT
    account_key,
    account_id,
    account_type,
    account_open_date,
    account_status,
    customer_key,
    customer_id,
    branch_key,
    branch_id
FROM deduplicated
WHERE row_num = 1
