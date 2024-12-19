WITH agg AS (
    SELECT
        post_id     AS "Post ID",
        'Boost'     AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_boost
    WHERE removed = FALSE
        AND boost_level = 1
    UNION all
    SELECT
        post_id     AS "Post ID",
        'Highlight' AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_boost
    WHERE removed = FALSE
        AND boost_level = 2
    UNION all
    SELECT
        post_id     AS "Post ID",
        'Comment'   AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_comment
    WHERE removed = FALSE
    UNION all  
    SELECT
        post_id     AS "Post ID",
        'Like'      AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_like
    WHERE comment_id IS NULL 
        AND boost_id IS NULL 
        AND reuse_id IS NULL
    UNION all
    SELECT
        post_id         AS "Post ID",
        'Comment Like'  AS "Action",
        user_id         AS "User ID",
        date            AS "Timestamp"
    FROM action_like
    WHERE comment_id IS NOT NULL    
    UNION all
    SELECT
        post_id     AS "Post ID",
        'Reuse'     AS "Action",
        user_id     AS "User ID",
        date        AS "Timestamp"
    FROM action_reuse
    WHERE removed = FALSE
    UNION all
    SELECT
        post_id     AS "Post ID",
        'View'      AS "Action",
        user_id     AS "User ID",
        view_date   AS "Timestamp"
    FROM action_view
    UNION all 
    SELECT 
        post_id     AS "Post ID",
        'Download'  AS "Action",
        user_id     AS "User ID",
        date_time   AS "Timestamp"
    FROM analytics.metabase_media_download_tracking
    WHERE source = 'post'
    UNION all 
    SELECT 
        post_id     AS "Post ID",
        'Boost 2 Download'  AS "Action",
        user_id     AS "User ID",
        date_time   AS "Timestamp"
    FROM analytics.metabase_media_download_tracking
    WHERE source = 'boost 2'
)

SELECT 
    agg.*,
    UDP.email                                      AS "Email"
FROM agg
    LEFT JOIN users_detail_with_preferences UDP 
        ON "User ID" = UDP.id