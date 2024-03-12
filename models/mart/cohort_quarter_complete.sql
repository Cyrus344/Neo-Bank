with subquery as (
SELECT 
DATE_TRUNC(user_transaction_device.created_date, QUARTER) AS user_quarter
,count(user_id) as nbr_user_per_quarter
,sum(nbr_transaction) as nbr_transaction_quarter
,COUNTIF(plan_price = 'free') AS free_plan_quarter
,COUNTIF(plan_price = 'paid') AS paid_plan_quarter
,COUNTIF(brand = 'Apple') AS Apple_quarter
,COUNTIF(brand = 'Android') AS Android_quarter
FROM {{ ref('user_transaction_device') }}
GROUP BY user_quarter
)

, subquery2 as (
SELECT 
DATE_TRUNC(user_transaction_device.created_date, QUARTER) AS user_quarter
,DATE_TRUNC(stg_neobank__transactions.created_date, QUARTER) AS transactions_quarter
,count(distinct(user_transaction_device.user_id)) as nbr_user_per_cohort
,count(distinct(stg_neobank__transactions.transaction_id)) as nbr_transaction_cohort
,date_diff(stg_neobank__transactions.created_date,user_transaction_device.created_date,QUARTER) as quarter_diff
,COUNTIF(plan_price = 'free') AS trs_free_plan_cohort
,COUNTIF(plan_price = 'paid') AS trs_paid_plan_cohort
,COUNTIF(brand = 'Apple') AS trs_Apple_cohort
,COUNTIF(brand = 'Android') AS trs_Android_cohort
FROM {{ ref('stg_neobank__transactions') }}
JOIN {{ ref('user_transaction_device') }}
USING(user_id)
where t_state LIKE 'COMPLETED'
GROUP BY user_quarter, transactions_quarter, quarter_diff
ORDER BY user_quarter, transactions_quarter
)

,subquery3 as (
SELECT
DATE_TRUNC(stg_neobank__users.created_date, quarter) AS user_quarter
,DATE_TRUNC(stg_neobank__transactions.created_date, quarter) AS transactions_quarter
,ROUND(sum(amount_usd),2) as total_spent
FROM {{ ref('stg_neobank__users') }}
LEFT JOIN {{ ref('stg_neobank__transactions') }}
USING(user_id)
WHERE direction LIKE 'OUTBOUND' AND t_state LIKE 'COMPLETED'
GROUP by user_quarter,transactions_quarter
ORDER BY user_quarter,transactions_quarter
)

,subquery4 as (
SELECT *
,nbr_user_per_cohort/nbr_user_per_quarter as percent_active_client
FROM subquery
JOIN subquery2
USING(user_quarter)
ORDER BY user_quarter, transactions_quarter
)

,subquery5 as(
SELECT
DATE_TRUNC(stg_neobank__users.created_date, quarter) AS user_quarter
,user_id
--,DATE_TRUNC(stg_neobank__device.created_date, MONTH) AS transactions_month
--,COUNTIF(brand = 'Apple')
--,COUNTIF(brand = 'Android')
,brand
FROM {{ ref('stg_neobank__users') }}
JOIN {{ ref('stg_neobank__devices') }}
USING(user_id)
)

,subquery8 as (
SELECT
user_quarter
,DATE_TRUNC(stg_neobank__transactions.created_date, quarter) AS transactions_quarter
,user_id
,brand
FROM subquery5
LEFT JOIN {{ ref('stg_neobank__transactions') }}
USING(user_id)
)

,subquery9 as (
SELECT 
user_quarter
,transactions_quarter
,count(distinct(user_id)) as nbr_apl_user
FROM subquery8
where brand like 'Apple'
GROUP BY user_quarter,transactions_quarter
)

,subquery10 as (
SELECT 
user_quarter
,transactions_quarter
,count(distinct(user_id)) as nbr_andr_user
FROM subquery8
where brand like 'Android'
GROUP BY user_quarter,transactions_quarter
)

,subquery11 as (SELECT *
FROM subquery10
JOIN subquery9
USING(user_quarter,transactions_quarter)
ORDER BY user_quarter,transactions_quarter
)

,subquery12 as (
SELECT *
FROM subquery4
JOIN subquery11
USING(user_quarter,transactions_quarter)
ORDER BY user_quarter, transactions_quarter
)
SELECT *
FROM subquery12
JOIN subquery3
USING(user_quarter, transactions_quarter)
ORDER BY user_quarter, transactions_quarter