with subquery as(
SELECT 
DATE_TRUNC(user_transaction_device.created_date, MONTH) AS user_month
,DATE_TRUNC(stg_neobank__notifications.created_date, MONTH) AS notifications_month
,channel
,count(user_id) as nbr_notif
,COUNTIF(status = 'SENT') AS nbr_sent
,COUNTIF(status = 'FAILED') AS nbr_failed
FROM {{ ref('user_transaction_device') }}
LEFT JOIN {{ ref('stg_neobank__notifications') }}
USING(user_id)
WHERE channel NOT LIKE 'SMS'
GROUP BY user_month,notifications_month,channel
ORDER BY user_month,notifications_month,channel
)
SELECT *
,nbr_sent/nbr_notif as percent_sent
FROM subquery