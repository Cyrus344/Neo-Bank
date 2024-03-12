with subquery as (
SELECT 
DATE_TRUNC(stg_neobank__users.created_date, QUARTER) AS user_quarter
,count(user_id) as nbr_clients
FROM {{ ref('stg_neobank__users') }}
GROUP BY user_quarter
)

, subquery2 as (
SELECT 
DATE_TRUNC(stg_neobank__users.created_date, quarter) AS user_quarter
,DATE_TRUNC(stg_neobank__transactions.created_date, quarter) AS transactions_quarter
,count(distinct(stg_neobank__users.user_id)) as nbr_user
,count(distinct(stg_neobank__transactions.transaction_id)) as nbr_transaction
,count(distinct(stg_neobank__transactions.transaction_id))/count(distinct(stg_neobank__users.user_id)) as t_per_user
,date_diff(stg_neobank__transactions.created_date,stg_neobank__users.created_date,QUARTER) as quarter_diff
FROM {{ ref('stg_neobank__transactions') }}
JOIN {{ ref('stg_neobank__users') }}
USING(user_id)
where t_state = "COMPLETED"
GROUP BY user_quarter, transactions_quarter, quarter_diff
ORDER BY user_quarter, transactions_quarter
)
SELECT *
,nbr_user/nbr_clients as percent_active_client
FROM subquery
JOIN subquery2
USING(user_quarter)
ORDER BY user_quarter, transactions_quarter