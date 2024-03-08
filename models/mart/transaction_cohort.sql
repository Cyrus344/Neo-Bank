WITH month_year AS (
    SELECT
        user_id,
        created_date,
        EXTRACT(YEAR FROM created_date) AS year,
        EXTRACT(MONTH FROM created_date) AS month
    FROM {{ ref('stg_neobank__users') }}
)
,cohort_month AS (
    SELECT
        user_id,
        created_date AS user_created_date,
        CONCAT(year,"/",month) AS cohort
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