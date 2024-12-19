WITH login AS (
    SELECT 
        userid,
        COUNT(userid) AS Total_login_count,
        COALESCE(MAX(date_time), CURRENT_DATE - INTERVAL '180 days') AS Last_login,
        CAST(EXTRACT(DAY FROM CURRENT_DATE - COALESCE(MAX(date_time), CURRENT_DATE - INTERVAL '180 days')) AS INTEGER) AS Days_since_last_login,
        COALESCE(CAST(EXTRACT(DAY FROM CURRENT_DATE - MIN(date_time)) AS INTEGER), 0) AS Age_on_platform
    FROM 
        metabase_login_report
    WHERE action = 'LOGIN'
        AND email NOT LIKE '%wegrow-app.com'
    GROUP BY
       userid 
),
agg AS (
    SELECT
        post_id     AS "Post ID",
        'Boost'     AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_boost
    WHERE removed = FALSE AND boost_level = 1
    UNION ALL
    SELECT
        post_id     AS "Post ID",
        'Highlight' AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_boost
    WHERE removed = FALSE AND boost_level = 2
    UNION ALL
    SELECT
        post_id     AS "Post ID",
        'Comment'   AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_comment
    WHERE removed = FALSE
    UNION ALL  
    SELECT
        post_id     AS "Post ID",
        'Like'      AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_like
    WHERE comment_id IS NULL AND boost_id IS NULL AND reuse_id IS NULL
    UNION ALL
    SELECT
        post_id         AS "Post ID",
        'Comment Like'  AS "Action",
        user_id         AS "User ID",
        date            AS "Timestamp"
    FROM action_like
    WHERE comment_id IS NOT NULL
    UNION ALL
    SELECT
        post_id     AS "Post ID",
        'Reuse'     AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_reuse
    WHERE removed = FALSE
    UNION ALL
    SELECT
        post_id     AS "Post ID",
        'View'      AS "Action",
        user_id     AS "User ID",
        view_date   AS "Timestamp"
    FROM action_view
    UNION ALL 
    SELECT 
        post_id     AS "Post ID",
        'Download'  AS "Action",
        user_id     AS "User ID",
        date_time   AS "Timestamp"
    FROM analytics.metabase_media_download_tracking
    WHERE source = 'post'
    UNION ALL 
    SELECT 
        post_id     AS "Post ID",
        'Boost 2 Download'  AS "Action",
        user_id     AS "User ID",
        date_time   AS "Timestamp"
    FROM analytics.metabase_media_download_tracking
    WHERE source = 'boost 2'
),
last_interaction AS (
    SELECT DISTINCT ON ("User ID")
        "User ID",
        "Action",
        "Post ID",
        "Timestamp"
    FROM agg
    ORDER BY "User ID", "Timestamp" DESC
)

SELECT 
    users.id AS userid,
    CASE
        WHEN users.firstname LIKE '%.%' THEN
            initcap(split_part(users.firstname, '.', 1)) || ' ' || initcap(split_part(users.firstname, '.', 2))
        WHEN users.lastname IS NOT NULL 
            AND users.lastname <> '' THEN
            initcap(users.firstname) || ' ' || initcap(users.lastname)
        ELSE
            initcap(users.firstname)
    END AS Name,
    users.level AS Level,
    users.email AS Email,
    users.email_like_frequency AS EmailFreq,
    COALESCE(users.country, 'Unspecified') AS Country,
    COALESCE(users.department, 'Unspecified') AS Department,
    login.Total_login_count,
    login.Last_login,
    login.Days_since_last_login,
    login.Age_on_platform,
    COALESCE(last_interaction."Action", 'No Interaction') AS Last_Action,
    COALESCE(last_interaction."Timestamp", login.Last_login) AS Last_Interaction_Date,
    COALESCE(last_interaction."Post ID", '0') AS Last_Interaction_PostID
FROM 
    users_detail_with_preferences AS users
LEFT JOIN login ON users.id = login.userid
LEFT JOIN last_interaction ON users.id = last_interaction."User ID";