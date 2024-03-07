with in_out_bound as (
SELECT 
user_id
,CASE
WHEN direction LIKE 'INBOUND' THEN amount_usd
ELSE 0
end as INBOUND
,CASE
WHEN direction LIKE 'OUTBOUND' THEN amount_usd
ELSE 0
end as OUTBOUND
FROM {{ ref('stg_neobank__transactions') }}
WHERE transactions_state LIKE 'COMPLETED'
)
, balance_update as (
SELECT
user_id
,sum(INBOUND) as sum_inbound
,sum(OUTBOUND) as sum_outbound
,sum(INBOUND) - sum(OUTBOUND) as balance
,CASE
WHEN sum(INBOUND) - sum(OUTBOUND) > 0 THEN "balanced budget"
ELSE "spends too much"
end as balance_state
FROM in_out_bound
GROUP BY user_id
)
SELECT
user_id
,sum_inbound
,sum_outbound
,balance
,balance_state
,plan_price
,age_category
,age
,nbr_transaction
,avg_transaction
FROM balance_update
JOIN {{ ref('user_transaction_device') }}
USING(user_id)