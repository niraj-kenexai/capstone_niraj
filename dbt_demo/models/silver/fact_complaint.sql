{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        complaint_id,
        complaint_category,
        complaint_priority,
        complaint_status,
        customer_id,
        branch_id
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE complaint_id IS NOT NULL AND TRIM(complaint_id) <> ''

),

cleaned AS (

    SELECT
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(complaint_id)), 2) AS complaint_key,
        TRY_CAST(TRIM(complaint_id) AS VARCHAR(50)) AS complaint_id,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(customer_id)), 2) AS customer_key,
        TRY_CAST(TRIM(customer_id) AS VARCHAR(50)) AS customer_id,
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', TRIM(branch_id)), 2) AS branch_key,
        TRY_CAST(TRIM(branch_id) AS VARCHAR(25)) AS branch_id,
        TRY_CAST(NULLIF(TRIM(complaint_category), '') AS VARCHAR(50)) AS complaint_category,
        TRY_CAST(NULLIF(TRIM(complaint_priority), '') AS VARCHAR(25)) AS complaint_priority,
        TRY_CAST(NULLIF(TRIM(complaint_status), '') AS VARCHAR(25)) AS complaint_status
    FROM source_data

),

deduplicated AS (

    SELECT
        complaint_key,
        complaint_id,
        customer_key,
        customer_id,
        branch_key,
        branch_id,
        complaint_category,
        complaint_priority,
        complaint_status,
        ROW_NUMBER() OVER (
            PARTITION BY complaint_id
            ORDER BY complaint_status DESC
        ) AS row_num
    FROM cleaned

)

SELECT
    complaint_key,
    complaint_id,
    customer_key,
    customer_id,
    branch_key,
    branch_id,
    complaint_category,
    complaint_priority,
    complaint_status
FROM deduplicated
WHERE row_num = 1
