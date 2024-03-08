
with year_q as (
SELECT
user_id
,EXTRACT(QUARTER FROM created_date) AS quarter
,EXTRACT(YEAR FROM created_date) AS year
FROM {{ ref('stg_neobank__users') }}
)
, table as (
SELECT
user_id
,CONCAT(year,"-Q",quarter) as year_quarter
FROM year_q
)
SELECT *
FROM table
JOIN {{ ref('user_balance') }}
USING(user_id)