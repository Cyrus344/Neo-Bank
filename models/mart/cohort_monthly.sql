SELECT 
DATE_TRUNC(stg_neobank__users.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__transactions.created_date, MONTH) AS transactions_month
,count(distinct(stg_neobank__users.user_id)) as nbr_user
FROM {{ ref('stg_neobank__transactions') }}
JOIN {{ ref('stg_neobank__users') }}
USING(user_id)
where transactions_state = "COMPLETED"
GROUP BY user_month, transactions_month
ORDER BY user_month, transactions_month