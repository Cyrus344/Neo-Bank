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
DATE_TRUNC(notif_date,MONTH) as notif_month
,DATE_TRUNC(user_date,MONTH) as user_month
,date_diff(notif_date,user_date,MONTH) as month_diff
,channel
,status
FROM subquery
)

SELECT
user_month
,notif_month
,month_diff
,channel
,count(month_diff) as nbr_notif
,COUNTIF(status = 'SENT') AS nbr_sent
,COUNTIF(status = 'FAILED') AS nbr_failed
,SAFE_DIVIDE (COUNTIF(status = 'SENT'), count(month_diff)) as prct_sent 
FROM subquery2
WHERE channel NOT LIKE 'SMS'
GROUP BY user_month,notif_month,month_diff,channel
ORDER BY user_month,notif_month,month_diff,channel