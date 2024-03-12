{{ config(materialized="table")}}
with age_cat as (
    SELECT *
    ,2019 - birth_year as age
    FROM {{ ref('int_user_transaction_2') }}
)
SELECT *
,CASE
WHEN age < 25 THEN "18-24"
WHEN age < 35 THEN "25-34"
WHEN age < 45 THEN "35-44"
WHEN age < 55 THEN "45-54"
WHEN age < 65 THEN "55-64"
else "65+"
end as age_category
,CASE
WHEN plan LIKE "Premium" THEN "paid"
WHEN plan LIKE "Metal" THEN "paid"
ELSE "free"
end as plan_price
FROM age_cat