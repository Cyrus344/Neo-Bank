SELECT
user_id
,(COUNTIF(t_state = 'COMPLETED')/COUNT(*)) AS complete_trans_rate
FROM {{ ref('stg_neobank__transactions') }}
GROUP BY user_id