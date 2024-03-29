WITH subquery AS (
SELECT 
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,count(user_id) as nbr_clients
FROM {{ ref('stg_neobank__users') }}
GROUP BY user_month
)

,subquery2 AS (
SELECT 
DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,count(distinct(stg_neobank__transactions.transaction_id)) as nbr_transaction
,count(distinct(stg_neobank__transactions.user_id)) as active_users
,sum (stg_neobank__transactions.amount_usd) as amount_spent
FROM {{ ref('stg_neobank__transactions') }}
where t_state = "COMPLETED" AND direction = "OUTBOUND"
GROUP BY transactions_month
ORDER BY transactions_month
)

SELECT 
*
,sum(nbr_clients) OVER (ORDER BY user_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_users
,SAFE_DIVIDE (active_users, sum(nbr_clients) OVER (ORDER BY user_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as prct_active_users
FROM subquery2 as subquery2
LEFT JOIN subquery as subquery
ON subquery.user_month = subquery2.transactions_month
ORDER BY transactions_month