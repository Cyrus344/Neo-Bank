with subquery as (
SELECT 
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,count(user_id) as nbr_clients
FROM {{ ref('stg_neobank__users') }}
GROUP BY user_month
)

, subquery2 as (
SELECT 
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,count(distinct(stg_neobank__users.user_id)) as nbr_user
,count(distinct(stg_neobank__transactions.transaction_id)) as nbr_transaction
,count(distinct(stg_neobank__transactions.transaction_id))/count(distinct(stg_neobank__users.user_id)) as t_per_user
,date_diff(stg_neobank__transactions.created_date,stg_neobank__users.created_date,MONTH) as month_diff
FROM {{ ref('stg_neobank__transactions') }}
JOIN {{ ref('stg_neobank__users') }}
USING(user_id)
where t_state = "COMPLETED"
GROUP BY user_month, transactions_month, month_diff
ORDER BY user_month, transactions_month
)
SELECT *
,nbr_user/nbr_clients as percent_active_client
FROM subquery
JOIN subquery2
USING(user_month)
ORDER BY user_month, transactions_month
