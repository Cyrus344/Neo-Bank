with subquery as (
SELECT 
DATE_TRUNC(user_transaction_device.created_date, MONTH) AS user_month
,count(user_id) as nbr_user_per_month
,sum(nbr_transaction) as nbr_transaction_month
,COUNTIF(plan_price = 'free') AS free_plan_month
,COUNTIF(plan_price = 'paid') AS paid_plan_month
,COUNTIF(brand = 'Apple') AS Apple_month
,COUNTIF(brand = 'Android') AS Android_month
FROM {{ ref('user_transaction_device') }}
GROUP BY user_month
)

, subquery2 as (
SELECT 
DATE_TRUNC(user_transaction_device.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,count(distinct(user_transaction_device.user_id)) as nbr_user_per_cohort
,count(distinct(stg_neobank__transactions.transaction_id)) as nbr_transaction_cohort
,date_diff(stg_neobank__transactions.created_date,user_transaction_device.created_date,MONTH) as month_diff
,COUNTIF(plan_price = 'free') AS trs_free_plan_cohort
,COUNTIF(plan_price = 'paid') AS trs_paid_plan_cohort
,COUNTIF(brand = 'Apple') AS trs_Apple_cohort
,COUNTIF(brand = 'Android') AS trs_Android_cohort
FROM {{ ref('stg_neobank__transactions') }}
LEFT JOIN {{ ref('user_transaction_device') }}
USING(user_id)
where t_state LIKE 'COMPLETED'
GROUP BY user_month, transactions_month, month_diff
ORDER BY user_month, transactions_month
)

,subquery3 as (
SELECT
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,ROUND(sum(amount_usd),2) as total_spent
FROM {{ ref('stg_neobank__users') }}
LEFT JOIN {{ ref('stg_neobank__transactions') }}
USING(user_id)
WHERE direction LIKE 'OUTBOUND' AND t_state LIKE 'COMPLETED'
GROUP by user_month,transactions_month
ORDER BY user_month,transactions_month
)

,subquery4 as (
SELECT *
,nbr_user_per_cohort/nbr_user_per_month as percent_active_client
FROM subquery
LEFT JOIN subquery2
USING(user_month)
ORDER BY user_month, transactions_month
)

,subquery5 as(
SELECT
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,user_id
--,DATE_TRUNC(stg_neobank__device.created_date, MONTH) AS transactions_month
--,COUNTIF(brand = 'Apple')
--,COUNTIF(brand = 'Android')
,brand
FROM {{ ref('stg_neobank__users') }}
LEFT JOIN {{ ref('stg_neobank__devices') }}
USING(user_id)
)

,subquery8 as (
SELECT
user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,user_id
,brand
FROM subquery5
LEFT JOIN {{ ref('stg_neobank__transactions') }}
USING(user_id)
)

,subquery9 as (
SELECT 
user_month
,transactions_month
,count(distinct(user_id)) as nbr_apl_user
FROM subquery8
where brand like 'Apple'
GROUP BY user_month,transactions_month
)

,subquery10 as (
SELECT 
user_month
,transactions_month
,count(distinct(user_id)) as nbr_andr_user
FROM subquery8
where brand like 'Android'
GROUP BY user_month,transactions_month
)

,subquery11 as (SELECT *
FROM subquery10
LEFT JOIN subquery9
USING(user_month,transactions_month)
ORDER BY user_month,transactions_month
)

,subquery13 as (
SELECT
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,user_id
,t_state
,CASE
WHEN plan LIKE 'Premium' THEN 'paid'
WHEN plan LIKE 'Metal' THEN 'paid'
ELSE 'free'
END AS plan_price
FROM {{ ref('stg_neobank__transactions') }}
LEFT JOIN {{ ref('stg_neobank__users') }}
USING(user_id)
)

,subquery18 as (
SELECT
user_month
,transactions_month
,count(distinct(user_id)) as nbr_paid_user_cohort
FROM subquery13
WHERE plan_price LIKE 'paid' AND t_state LIKE 'COMPLETED'
GROUP BY user_month,transactions_month
ORDER BY user_month,transactions_month
)

,subquery16 as (
SELECT
user_month
,transactions_month
,count(distinct(user_id)) as nbr_free_user_cohort
FROM subquery13
WHERE plan_price LIKE 'free' AND t_state LIKE 'COMPLETED'
GROUP BY user_month,transactions_month
ORDER BY user_month,transactions_month
)

,subquery17 as (
SELECT *
FROM subquery18
LEFT JOIN subquery16
USING(user_month,transactions_month)
ORDER BY user_month,transactions_month
)

,subquery15 as(
SELECT
user_month
,transactions_month
,month_diff
,nbr_user_per_month
,nbr_user_per_cohort
,percent_active_client
,free_plan_month
,paid_plan_month
,nbr_transaction_month
,nbr_transaction_cohort
,round(nbr_transaction_cohort/nbr_transaction_month,3) as percent_transaction_cohort
FROM subquery4
LEFT JOIN subquery11
USING(user_month,transactions_month)
)
SELECT *
FROM subquery15
LEFT JOIN subquery17
USING(user_month,transactions_month)
ORDER BY user_month,month_diff