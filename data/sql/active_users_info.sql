WITH UserFirstLogin AS (
    SELECT 
        userid,
        MIN(date_time) AS first_login_date
    FROM metabase_login_report
    GROUP BY userid
),
LastAction AS (
    SELECT
        userid,
        MAX(date) AS last_action_time,
        action_type
    FROM (
        SELECT userid, date_time AS date, 'login' AS action_type
        FROM metabase_login_report
        WHERE date_time BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
            AND action = 'LOGIN'
            AND email NOT LIKE '%wegrow-app.com'
        UNION ALL
        SELECT user_id AS userid, view_date AS date, 'view' AS action_type
        FROM action_view
        WHERE view_date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
        UNION ALL
        SELECT user_id AS userid, date, 'like' AS action_type
        FROM action_like
        WHERE date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
        UNION ALL
        SELECT user_id AS userid, date, 'boost' AS action_type
        FROM action_boost
        WHERE date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
        UNION ALL
        SELECT user_id AS userid, date, 'reuse' AS action_type
        FROM action_reuse
        WHERE date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
    ) actions
    GROUP BY userid, action_type
),

MaxAction AS (
    SELECT
        userid,
        MAX(last_action_time) AS final_last_action_time
    FROM LastAction
    GROUP BY userid
),

UserLastAction AS (
    SELECT
        LastAction.userid,
        LastAction.action_type AS last_action
    FROM LastAction
    JOIN MaxAction ON LastAction.userid = MaxAction.userid AND LastAction.last_action_time = MaxAction.final_last_action_time
),

AggregatedActions AS (
    SELECT
        userid,
        COUNT(*) FILTER (WHERE action_type = 'view') AS views_given,
        COUNT(*) FILTER (WHERE action_type = 'like') AS likes_given,
        COUNT(*) FILTER (WHERE action_type = 'boost') AS boosts_given,
        COUNT(*) FILTER (WHERE action_type = 'reuse') AS reuses_given
    FROM (
        SELECT user_id AS userid, 'view' AS action_type FROM action_view
        UNION ALL
        SELECT user_id AS userid, 'like' AS action_type FROM action_like
        UNION ALL
        SELECT user_id AS userid, 'boost' AS action_type FROM action_boost
        UNION ALL
        SELECT user_id AS userid, 'reuse' AS action_type FROM action_reuse
    ) actions
    GROUP BY userid
)

SELECT 
    login.userid,
    COUNT(*) AS total_login_count,
    details.email_like_frequency AS email_freq,
    details.firstname,
    details.lastname,
    details.level,
    details.email,
    COALESCE(details.country, 'Unspecified') AS country,
    COALESCE(details.department, 'Unspecified') AS department,
    MAX(login.date_time) AS last_login_date,
    EXTRACT(DAY FROM CURRENT_DATE - MAX(login.date_time)) AS days_since_last_login,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, UserFirstLogin.first_login_date)) * 12 +
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, UserFirstLogin.first_login_date)) +
    EXTRACT(DAY FROM AGE(CURRENT_DATE, UserFirstLogin.first_login_date)) / 30.0 AS months_on_platform,
    COALESCE(COUNT(*) / NULLIF(EXTRACT(YEAR FROM AGE(CURRENT_DATE, UserFirstLogin.first_login_date)) * 12 +
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, UserFirstLogin.first_login_date)), 0), 0) AS login_frequency,
    COALESCE(aggactions.views_given, 0) AS total_views_given,
    COALESCE(aggactions.likes_given, 0) AS total_likes_given,
    COALESCE(aggactions.boosts_given, 0) AS total_boosts_given,
    COALESCE(aggactions.reuses_given, 0) AS total_reuses_given,
    COALESCE(UserLastAction.last_action, 'login') AS last_action
FROM metabase_login_report login
LEFT JOIN users_detail_with_preferences details ON login.userid = details.id
LEFT JOIN UserFirstLogin ON login.userid = UserFirstLogin.userid
LEFT JOIN UserLastAction ON login.userid = UserLastAction.userid
LEFT JOIN AggregatedActions aggactions ON login.userid = aggactions.userid
WHERE login.date_time BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
    AND login.action = 'LOGIN'
    AND login.email NOT LIKE '%wegrow-app.com'
GROUP BY login.userid, details.email_like_frequency, details.firstname, details.lastname, details.level, details.email, details.country, details.department, UserFirstLogin.first_login_date, aggactions.views_given, aggactions.likes_given, aggactions.boosts_given, aggactions.reuses_given, UserLastAction.last_action;