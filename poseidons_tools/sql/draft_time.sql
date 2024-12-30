WITH status_fix AS (
    SELECT
        id,
        user_id,
        CASE
            WHEN draft = TRUE THEN 'DRAFT'
            WHEN draft = FALSE THEN 'PUBLISHED'
            ELSE status
        END AS status,
        created_date,
        creation_date
    FROM post_activity
),

lagged AS (
    SELECT
        status_fix.id,
        LAG(status_fix.id) OVER (PARTITION BY status_fix.id ORDER BY status_fix.creation_date ASC) AS lagged_id,
        status_fix.status,
        LAG(status_fix.status) OVER (PARTITION BY status_fix.id ORDER BY status_fix.creation_date ASC) AS lagged_status,
        status_fix.created_date AS first_draft,
        status_fix.creation_date AS first_publish
    FROM 
        status_fix
        JOIN users_detail_with_preferences UDP
            ON status_fix.user_id = UDP.id        
    WHERE
        status_fix.status IS NOT NULL
        AND UDP.email NOT LIKE '%wegrow%'
),

unique_posts AS (
    SELECT
        DISTINCT id,
        first_draft,
        CASE WHEN status = 'PUBLISHED' AND lagged_status = 'DRAFT' AND id = lagged_id THEN MIN(first_publish) OVER (PARTITION BY id)
            END AS first_publish
    FROM lagged
    WHERE (status = 'PUBLISHED' AND lagged_status = 'DRAFT' AND id = lagged_id)
),

minute_diff AS (
    SELECT
        id,
        first_publish,
        first_draft,
        EXTRACT(EPOCH FROM (first_publish - first_draft)) / 60 AS post_creation_time
    FROM unique_posts
)

SELECT
    (SELECT COUNT(DISTINCT id) FROM status_fix)                             AS total_posts_so_far,
    COUNT(*)                                                                AS sample_size,
    AVG(post_creation_time)                                                 AS average,
    PERCENTILE_CONT(0.2) WITHIN GROUP(ORDER BY post_creation_time)          AS "20th_minute_diff",
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY post_creation_time)          AS median_minute_diff,
    PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY post_creation_time)          AS "80th_minute_diff",
    MAX(post_creation_time)                                                 AS max_minute_diff,
    MIN(post_creation_time)                                                 AS min_minute_diff
FROM minute_diff