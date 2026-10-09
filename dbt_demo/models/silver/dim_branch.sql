{{ config(materialized='view', schema='silver') }}

WITH source_data AS (

    SELECT
        TRIM(branch_id) AS branch_id,
        TRIM(branch_name) AS branch_name
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE branch_id IS NOT NULL AND TRIM(branch_id) <> ''

),

deduplicated AS (

    SELECT
        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', branch_id), 2) AS branch_key,
        TRY_CAST(branch_id AS VARCHAR(25)) AS branch_id,
        TRY_CAST(branch_name AS VARCHAR(100)) AS branch_name,
        ROW_NUMBER() OVER (
            PARTITION BY branch_id
            ORDER BY LEN(branch_name) DESC
        ) AS row_num
    FROM source_data

)

SELECT
    branch_key,
    branch_id,
    branch_name
FROM deduplicated
WHERE row_num = 1
