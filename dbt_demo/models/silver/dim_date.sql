{{ config(materialized='view', schema='silver') }}

WITH dates AS (

    SELECT DISTINCT
        CAST(
            COALESCE(
                TRY_CONVERT(DATE, TRIM(transaction_date), 120),
                TRY_CONVERT(DATE, TRIM(transaction_date), 101),
                TRY_CONVERT(DATE, TRIM(transaction_date), 103),
                TRY_CAST(TRIM(transaction_date) AS DATE)
            ) AS DATE
        ) AS full_date
    FROM {{ source('bronze', 'Banking_Raw') }}
    WHERE transaction_date IS NOT NULL

)

SELECT
    CAST(FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    DAY(full_date) AS day,
    MONTH(full_date) AS month,
    YEAR(full_date) AS year,
    DATEPART(QUARTER, full_date) AS quarter,
    DATENAME(MONTH, full_date) AS month_name,
    FORMAT(full_date, 'yyyy-MM') AS year_month,
    DATENAME(WEEKDAY, full_date) AS day_of_week,
    CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM dates
WHERE full_date IS NOT NULL
