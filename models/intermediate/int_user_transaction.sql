SELECT 
user_id
,sum(amount_usd) as total_amount
,count(transaction_id) as nbr_transaction
,avg(amount_usd) as avg_transaction
FROM {{ ref('stg_neobank__transactions') }}
WHERE transactions_state LIKE "COMPLETED"
GROUP BY user_id