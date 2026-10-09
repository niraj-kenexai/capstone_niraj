{{ config(materialized='view', schema='gold') }}

SELECT
    b.branch_name,
    c.complaint_category,
    COUNT(c.complaint_id) AS total_complaints,
    SUM(CASE WHEN c.complaint_status IN ('Resolved', 'Closed') THEN 1 ELSE 0 END) AS resolved_complaints,
    ROUND(
        (SUM(CASE WHEN c.complaint_status IN ('Resolved', 'Closed') THEN 1.0 ELSE 0.0 END) * 100.0) /
        NULLIF(COUNT(c.complaint_id), 0),
        2
    ) AS resolution_rate_pct
FROM {{ ref('fact_complaint') }} c
JOIN {{ ref('dim_branch') }} b 
  ON c.branch_key = b.branch_key
WHERE c.complaint_category IS NOT NULL
GROUP BY b.branch_name, c.complaint_category
