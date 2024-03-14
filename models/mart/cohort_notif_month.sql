with subquery as(
SELECT 
CAST(stg_neobank__notifications.created_date AS datetime) as notif_date
,CAST(stg_neobank__users.created_date AS datetime) as user_date
,channel
,status
FROM {{ ref('stg_neobank__users') }}
LEFT JOIN {{ ref('stg_neobank__notifications') }}
USING(user_id)
)
,subquery2 as(
SELECT
DATE_TRUNC(notif_date,MONTH) as transactions_month
,DATE_TRUNC(user_date,MONTH) as user_month
,date_diff(notif_date,user_date,MONTH) as month_diff
,channel
,status
FROM subquery
)

,subquery24 as(
SELECT
user_month
,transactions_month
,month_diff
,count(month_diff) as nbr_notif
,COUNTIF(status = 'SENT') AS nbr_sent
,COUNTIF(status = 'FAILED') AS nbr_failed
,COUNTIF(channel = 'EMAIL') AS nbr_emails
,COUNTIF(channel = 'PUSH') AS nbr_pushs
,SAFE_DIVIDE (COUNTIF(status = 'SENT'), count(month_diff)) as prct_sent 
FROM subquery2
WHERE channel NOT LIKE 'SMS'
GROUP BY user_month,transactions_month,month_diff
ORDER BY user_month,transactions_month,month_diff
)
SELECT
user_month
,transactions_month
,subquery24.month_diff
,nbr_notif
,nbr_sent
,nbr_failed
,nbr_emails,nbr_pushs
,prct_sent
,nbr_user_per_cohort
,percent_active_client
,nbr_transaction_month
FROM subquery24
JOIN {{ ref('cohort_month_complete') }}
USING(user_month,transactions_month)
ORDER BY user_month,transactions_month,month_diff
