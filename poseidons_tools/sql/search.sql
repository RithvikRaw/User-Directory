WITH merged AS (
    -- Merging the two tables
    SELECT 
        *
    FROM analytics.metabase_report_search_without_terms
    WHERE CAST(param_arrays AS varchar) NOT LIKE '%ALL_MY_PREFERENCES%' -- Excluding views of users' own profile
        AND CAST(param_arrays AS varchar) NOT LIKE '%postStatus%' -- Excluding views of users' own posts
        AND CAST(param_arrays AS varchar) LIKE '%order%'
        AND CAST(param_arrays AS varchar) NOT LIKE '%order=rating%'
        AND CAST(param_arrays AS varchar) NOT LIKE '%order=pertinence%' -- Searches always have an 'order' parameter, for w/o term searches, the default is always 'date', we can safely
                                                                        -- remove the others under the assumption that they are from the same search session.
    UNION ALL 
    SELECT 
        * 
    FROM analytics.metabase_report_search_with_terms
    WHERE CAST(param_arrays AS varchar) LIKE '%order%'
        AND CAST(param_arrays AS varchar) NOT LIKE '%order=date%'
        AND CAST(param_arrays AS varchar) NOT LIKE '%order=rating%' -- Searches always have an 'order' parameter, for w term searches, the default is always 'pertinence', we can safely
                                                                    -- remove the others under the assumption that they are from the same search session.
    -- Small note: the 'size' parameter is determined by screen size.
),
merged_wid AS (
    -- Adding an id column
    SELECT *, ROW_NUMBER() OVER (ORDER BY pre_handle_date_time ASC) AS id
    FROM merged
    WHERE (CAST(param_arrays AS varchar) NOT LIKE '%page=%' OR CAST(param_arrays AS varchar) LIKE '%page=0%') -- Removing the same search query duplicated by pagination
),
lagged_searches AS (
    -- Intermediate step to identify unique searches
    SELECT *, LAG(param_arrays) OVER (PARTITION BY auth_user ORDER BY pre_handle_date_time ASC)
    FROM merged_wid
),
unique_searches AS (
    -- Identifying unique searches
    SELECT *, CASE WHEN lag <> param_arrays OR lag IS NULL THEN TRUE ELSE FALSE END AS unique_search
    FROM lagged_searches
    WHERE CASE WHEN lag <> param_arrays OR lag IS NULL THEN TRUE ELSE FALSE END = TRUE
),
split_arr AS (
    -- Splitting the parameters from the array
    SELECT auth_user, CAST(param_arrays AS varchar) as param_arrays, unnest(param_arrays) as "parameters", pre_handle_date_time, id
    FROM unique_searches
),
date_values AS ( 
    -- Ref table for date values
    SELECT * FROM (
        VALUES 
            (0, 'Anytime'),
            (1, 'Last 24 hours'),
            (2, 'This week'),
            (3, 'This month'),
            (4, 'This year'),
            (5, 'Last 5 years')
    ) AS temp("value", label)
),
search_history AS (
    -- Splitting the parameters into key-value pairs
    SELECT
        *,
        CASE WHEN position('=' in parameters) > 0 THEN split_part(parameters, '=', 1) ELSE 'term' END AS param,
        CASE WHEN position('=' in parameters) > 0 THEN split_part(parameters, '=', 2) ELSE parameters END AS value
    FROM split_arr
),
reuse_ref AS (
    -- Ref table for reuse 
    SELECT id, (regexp_matches(param_arrays, 'relatedToProfileId=([0-9]+)', 'g'))[1] AS value 
    FROM search_history
    WHERE param_arrays LIKE '%reused%' AND param_arrays LIKE '%relatedToProfileId%' AND param = 'reused'
),
merged_search_history AS (
    -- Enriching the search history with additional information
    SELECT 
        SHI.*,
        CASE WHEN SHI.param = 'checklistItems' THEN 'checklist items'
            WHEN SHI.param = 'read' THEN 'read by me'
            WHEN SHI.param = 'unread' THEN 'unread by me'
            WHEN SHI.param = 'liked' THEN 'liked by me'
            WHEN SHI.param = 'commented' THEN 'commented by me'
            WHEN SHI.param = 'bookmarkOnly' THEN 'bookmarked by me'
            WHEN SHI.param = 'contestIds' THEN 'contest'
            WHEN SHI.param = 'askReworked' THEN 'ask reworked posts'
            WHEN SHI.param = 'shared' THEN 'shared by me'
            WHEN SHI.param = 'date' THEN 'time frame'
            WHEN SHI.param_arrays LIKE '%boosted%' AND SHI.param_arrays LIKE '%boostedLevel=1%' AND SHI.param = 'boosted' THEN 'all boosted posts'
            WHEN SHI.param_arrays LIKE '%boosted%' AND SHI.param_arrays LIKE '%boostedLevel=2%' AND SHI.param = 'boosted' THEN 'all highlighted posts'
            WHEN SHI.param_arrays LIKE '%reused%' AND SHI.param_arrays LIKE '%reusedBy=-1%' AND SHI.param = 'reused' THEN 'reused by me'
            WHEN SHI.param_arrays LIKE '%reused%' AND SHI.param_arrays LIKE '%relatedToProfileId%' AND SHI.param = 'reused' THEN 'reused posts of user'
            WHEN SHI.param_arrays LIKE '%reused%' AND SHI.param_arrays NOT LIKE '%reusedBy=-1%' AND SHI.param = 'reused' THEN 'all reused posts'
            WHEN SHI.parameters = 'draft=true' AND SHI.param = 'draft' THEN 'my drafts'
            WHEN SHI.parameters = 'draft=false' AND SHI.param = 'draft' THEN 'my published posts'
            ELSE SHI.param 
        END AS "search_type",
        CASE WHEN SHI.param_arrays LIKE '%reused%' AND SHI.param_arrays LIKE '%relatedToProfileId%' AND SHI.param = 'reused' THEN UA1.email
            WHEN SHI.param = 'category' THEN TCA.label
            WHEN SHI.param = 'ability' THEN TAB.label
            WHEN SHI.param = 'strategy' THEN TST.label
            WHEN SHI.param = 'location' THEN TLO.label
            WHEN SHI.param = 'continent' THEN TCO.label
            WHEN SHI.param = 'country' THEN TCOU.label
            WHEN SHI.param = 'region' THEN TREG.label
            WHEN SHI.param = 'city' THEN TCI.label
            WHEN SHI.param = 'opportunity' THEN TOPP.label
            WHEN SHI.param = 'outcome' THEN TOU.label
            WHEN SHI.param = 'brand' THEN TBR.label
            WHEN SHI.param = 'checklistItems' THEN ACI.label
            WHEN SHI.param = 'user' THEN UAC.email
            WHEN SHI.param = 'contestIds' THEN GCO.name
            WHEN SHI.param = 'term' then SHI.value
            WHEN SHI.param = 'date' THEN DV.label
            ELSE null
        END AS "param_value"
    FROM search_history SHI
        LEFT JOIN tag_category TCA
            ON SHI.param = 'category' AND SHI.value = CAST(TCA.id AS varchar)
        LEFT JOIN tag_ability TAB 
            ON SHI.param = 'ability' AND SHI.value = CAST(TAB.id AS varchar)
        LEFT JOIN tag_strategy TST 
            ON SHI.param = 'strategy' AND SHI.value = CAST(TST.id AS varchar)
        LEFT JOIN tag_location TLO  
            ON SHI.param = 'location' AND SHI.value = CAST(TLO.id AS varchar)
        LEFT JOIN tag_continent TCO 
            ON SHI.param = 'continent' AND SHI.value = CAST(TCO.id AS varchar)
        LEFT JOIN tag_country TCOU
            ON SHI.param = 'country' AND SHI.value = CAST(TCOU.id AS varchar)
        LEFT JOIN tag_region TREG
            ON SHI.param = 'region' AND SHI.value = CAST(TREG.id AS varchar)
        LEFT JOIN tag_city TCI
            ON SHI.param = 'city' AND SHI.value = CAST(TCI.id AS varchar)
        LEFT JOIN tag_opportunity TOPP 
            ON SHI.param = 'opportunity' AND SHI.value = CAST(TOPP.id AS varchar)
        LEFT JOIN tag_outcome TOU 
            ON SHI.param = 'outcome' AND SHI.value = CAST(TOU.id AS varchar)
        LEFT JOIN tag_brand TBR 
            ON SHI.param = 'brand' AND SHI.value = CAST(TBR.id AS varchar)
        LEFT JOIN adaptive_checklist_item ACI 
            ON SHI.param = 'checklistItems' AND SHI.value = CAST(ACI.id AS varchar)
        LEFT JOIN user_account UAC 
            ON SHI.param = 'user' AND SHI.value = CAST(UAC.id AS varchar)
        LEFT JOIN gaming_contest GCO 
            ON SHI.param = 'contestIds' AND SHI.value = CAST(GCO.id AS varchar)
        LEFT JOIN date_values DV 
            ON SHI.param = 'date' AND SHI.value = CAST(DV.value AS varchar)
        LEFT JOIN reuse_ref RRE 
            ON SHI.id = RRE.id
        LEFT JOIN user_account UA1
            ON CAST(RRE.value AS bigint) = UA1.id
    -- Removing duplicates
    WHERE param NOT IN ('order', 'page', 'size', 'useCase', 'unreadBy', 'reusedBy', 'likedBy', 'boostedBy', 'boostedLevel', 'askReworkedBy', 'dateFrom', 'dateTo', 'relatedToProfileId', 'user')
    -- Removing duplicates for self searches
    AND parameters NOT IN ('user=-1')
    -- Removing odd cases in Campari
    AND param NOT LIKE 'https://%'
)

-- Final query
SELECT
    MSH.id                                          AS "Search ID",
    UAC.id                                          AS "User ID",
    UAC.email                                       AS "Email",
    pre_handle_date_time                            AS "Timestamp",
    param_arrays                                    AS "Raw Parameters",
    search_type                                     AS "Search Type",
    param_value                                     AS "Search Value",
    COALESCE(TCN.label, 'Unspecified')              AS "Continent",
    COALESCE(TCO.label, 'Unspecified')              AS "Geo Region",
    COALESCE(TDE.label, 'Unspecified')              AS "Department"
FROM merged_search_history MSH
    LEFT JOIN user_account UAC
        ON LOWER(MSH.auth_user) = LOWER(UAC.email)
    LEFT JOIN tag_country TCO 
        ON UAC.country = TCO.id
    LEFT JOIN tag_continent TCN 
        ON TCO.continent_id = TCN.id
    LEFT JOIN tag_department TDE 
        ON UAC.departement = TDE.id
ORDER BY MSH.id