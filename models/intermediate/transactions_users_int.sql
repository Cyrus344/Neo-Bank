SELECT 
user_id
,sum(amount_usd) as total_amount
,count(transaction_id) as nbr_transaction
,avg(amount_usd) as avg_transaction
FROM {{ ref('stg_cleaned__transactions') }}
WHERE transactions_state LIKE "COMPLETED"
GROUP BY user_id
--This query makes the total amount transferred and the number of transactions where the transaction was completed per user_id--