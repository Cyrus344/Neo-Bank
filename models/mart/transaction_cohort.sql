WITH month_year AS (
    SELECT
        user_id,
        created_date,
        EXTRACT(YEAR FROM created_date) AS yearj,
        EXTRACT(MONTH FROM created_date) AS monthj
    FROM {{ ref('stg_neobank__users') }}
)
,cohort_month AS (
    SELECT
        user_id,
        created_date AS user_created_date,
        COnCAT(yearj,monthj) AS cohort
    FROM month_year
)
, setup_join as (
SELECT
user_id
,user_created_date
,cohort
FROM cohort_month
)
SELECT *
FROM setup_join 
RIGHT JOIN {{ ref('stg_neobank__transactions') }}
USING(user_id)
ORDER BY cohort, created_date