WITH ActiveUsers AS (
    SELECT userid
    FROM metabase_login_report
    WHERE DATE(date_time) BETWEEN CURRENT_DATE - INTERVAL '180 days' AND CURRENT_DATE - INTERVAL '90 days'
      AND action = 'LOGIN'
      AND email NOT LIKE '%wegrow-app.com'
    GROUP BY userid
)
SELECT 
    au.userid,
    COUNT(login.*) AS total_login_count, -- Counting all logins for these users
    EXTRACT(DAY FROM (CURRENT_DATE - INTERVAL '90 days') - MAX(login.date_time)) AS days_since_last_login, 
    u.email_like_frequency AS email_freq
FROM ActiveUsers au
JOIN metabase_login_report login ON au.userid = login.userid
LEFT JOIN users_detail_with_preferences u ON au.userid = u.id
WHERE login.date_time < CURRENT_DATE - INTERVAL '90 days' -- Only logins before the last 90 days
  AND login.action = 'LOGIN'
GROUP BY au.userid, u.email_like_frequency;