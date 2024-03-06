SELECT 
users_id
,sum(amount_usd) as total_amount
,count(transaction_id) as nbr_transaction
FROM neobank-416209.neobank.transactions_cleaned
WHERE transactions_state LIKE "COMPLETED"
GROUP BY user_id
--This query makes the total amount transferred and the number of transactions where the transaction was completed per user_id