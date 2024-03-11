{{ config(materialized="table")}}
with age_cat as (
    SELECT *
    ,2019 - birth_year as age
    FROM {{ ref('int_user_transaction_2') }}
)
SELECT *
,CASE
WHEN age < 25 THEN "18-25"
WHEN age < 35 THEN "25-35"
WHEN age < 50 THEN "35-50"
else "50+"
end as age_category
,CASE
WHEN plan LIKE "Premium" THEN "paid"
WHEN plan LIKE "Metal" THEN "paid"
ELSE "free"
end as plan_price
FROM age_cat