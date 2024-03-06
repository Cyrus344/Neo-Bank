{{ config(materialized="table")}}
with michel as (
    SELECT *
    ,2019 - birth_year as age
    FROM {{ ref('users_transactions') }}
)
SELECT *
,CASE
WHEN age < 25 THEN "young"
WHEN age < 35 THEN "middle"
WHEN age < 50 THEN "wise"
else "senior"
end as age_category
,CASE
WHEN plan LIKE "Premium" THEN "paid"
WHEN plan LIKE "Metal" THEN "paid"
ELSE "free"
end as plan_price
FROM michel