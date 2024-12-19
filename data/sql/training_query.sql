WITH login AS (
    SELECT 
        userid,
        COUNT(userid) AS Total_login_count,
        CAST(EXTRACT(DAY FROM CURRENT_DATE - COALESCE(MAX(date_time), CURRENT_DATE - INTERVAL '180 days')) AS INTEGER) AS Days_since_last_login
    FROM 
        metabase_login_report
    WHERE action = 'LOGIN'
        AND email NOT LIKE '%wegrow-app.com'
    GROUP BY
       userid 
)

SELECT 
    login.userid AS userid,
    login.Total_login_count,
    login.Days_since_last_login,
    users.email_like_frequency AS email_freq
FROM 
    users_detail_with_preferences AS users
LEFT JOIN login ON users.id = login.userid