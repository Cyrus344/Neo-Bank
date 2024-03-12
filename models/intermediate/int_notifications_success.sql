SELECT
user_id
,COUNTIF(status = "SENT")/COUNT(*) AS pct_sent
FROM `neobank-416209.neobank.notifications`
GROUP BY user_id