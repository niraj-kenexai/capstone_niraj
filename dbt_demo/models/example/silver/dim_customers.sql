{{ config(materialized='view', schema='silver') }}

-- Mirror dim_customer for backward compatibility
SELECT * FROM {{ ref('dim_customer') }}