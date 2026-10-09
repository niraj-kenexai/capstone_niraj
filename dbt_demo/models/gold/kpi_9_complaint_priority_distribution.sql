{{ config(materialized='view', schema='gold') }}

SELECT
    b.branch_name,
    SUM(CASE WHEN c.complaint_priority = 'Critical' THEN 1 ELSE 0 END) AS critical_priority_complaints,
    SUM(CASE WHEN c.complaint_priority = 'High' THEN 1 ELSE 0 END) AS high_priority_complaints,
    SUM(CASE WHEN c.complaint_priority = 'Medium' THEN 1 ELSE 0 END) AS medium_priority_complaints,
    SUM(CASE WHEN c.complaint_priority = 'Low' THEN 1 ELSE 0 END) AS low_priority_complaints
FROM {{ ref('fact_complaint') }} c
JOIN {{ ref('dim_branch') }} b 
  ON c.branch_key = b.branch_key
GROUP BY b.branch_name
