SELECT 
    users.id AS userid,
    CASE
        WHEN users.firstname LIKE '%.%' THEN split_part(users.firstname, '.', 1)
        ELSE users.firstname
    END AS Firstname,
    CASE
        WHEN users.firstname LIKE '%.%' THEN split_part(users.firstname, '.', 2)
        ELSE users.lastname
    END AS Lastname,
    CASE
        WHEN users.firstname LIKE '%.%' THEN
            split_part(users.firstname, '.', 1) || ' ' || split_part(users.firstname, '.', 2)
        WHEN users.lastname IS NOT NULL AND users.lastname <> '' THEN
            users.firstname || ' ' || users.lastname
        ELSE
            users.firstname
    END AS Name,
    users.level AS Level,
    users.email AS Email,
    users.email_like_frequency AS EmailFreq,
    COALESCE(users.country, 'Unspecified') AS Country,
    COALESCE(users.department, 'Unspecified') AS Department,
    COUNT(login.userid) AS Total_login_count,
    COALESCE(MAX(login.date_time), CURRENT_DATE - INTERVAL '180 days') AS Last_login,
    CAST(EXTRACT(DAY FROM CURRENT_DATE - COALESCE(MAX(login.date_time), CURRENT_DATE - INTERVAL '180 days')) AS INTEGER) AS Days_since_last_login,
    COALESCE(CAST(EXTRACT(DAY FROM CURRENT_DATE - MIN(login.date_time)) AS INTEGER), 0) AS Age_on_platform
FROM 
    users_detail_with_preferences AS users
LEFT JOIN metabase_login_report AS login ON users.id = login.userid
WHERE login.action = 'LOGIN'
    AND login.email NOT LIKE '%wegrow-app.com'
GROUP BY 
    users.id, 
    users.firstname, 
    users.lastname, 
    users.level, 
    users.email, 
    users.email_like_frequency, 
    users.country, 
    users.department;