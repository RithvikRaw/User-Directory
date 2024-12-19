WITH merged AS (
    -- Merging the two tables
    SELECT 
        *
    FROM analytics.metabase_report_search_without_terms
    WHERE CAST(param_arrays AS varchar) NOT LIKE '%ALL_MY_PREFERENCES%' -- Excluding views of users' own profile
        AND CAST(param_arrays AS varchar) NOT LIKE '%postStatus%' -- Excluding views of users' own posts
        AND CAST(param_arrays AS varchar) LIKE '%order%'
        AND auth_user NOT LIKE '%wegrow%'
    UNION ALL 
    SELECT 
        * 
    FROM analytics.metabase_report_search_with_terms
    WHERE auth_user NOT LIKE '%wegrow%'
        AND CAST(param_arrays AS varchar) LIKE '%order%'
),
lagged_time AS (
    -- Intermediate step to identify unique searches
    SELECT *, LAG(pre_handle_date_time) OVER (PARTITION BY auth_user ORDER BY pre_handle_date_time ASC)
    FROM merged
),
search_time_cut_off AS (
    SELECT 
        *,
        SUM(
            CASE WHEN 
            DATE_PART('Day', pre_handle_date_time - COALESCE(lag, '2000-01-01')) * 24 + DATE_PART('Hour', pre_handle_date_time - COALESCE(lag, '2000-01-01')) * 60 + DATE_PART('Minute', pre_handle_date_time - COALESCE(lag, '2000-01-01')) <= 30
            THEN 0 ELSE 1 END
        ) OVER (ORDER BY auth_user, pre_handle_date_time ASC) AS "search_id"
    FROM lagged_time
),
search_time AS (
    SELECT search_id, first.auth_user, DATE_PART('Minute', last.pre_handle_date_time - first.pre_handle_date_time) AS minutes_to_search FROM (
        SELECT DISTINCT ON (search_id) auth_user, pre_handle_date_time, search_id
        FROM search_time_cut_off
        ORDER BY search_id, auth_user, pre_handle_date_time
    ) first
    JOIN (
        SELECT DISTINCT ON (search_id) auth_user, pre_handle_date_time, search_id
        FROM search_time_cut_off
        ORDER BY search_id, auth_user, pre_handle_date_time DESC
    ) last USING (search_id)

)
-- select * from search_time_cut_off
SELECT
    AVG(minutes_to_search) AS average_search_time,
    PERCENTILE_CONT(0.2) WITHIN GROUP(ORDER BY minutes_to_search) AS "20th_search_time",
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY minutes_to_search) AS median_search_time,
    PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY minutes_to_search) AS "80th_search_time",
    MAX(minutes_to_search) AS max_search_time,
    MIN(minutes_to_search) AS min_search_time
FROM search_time