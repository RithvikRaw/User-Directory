SELECT
    post.id                                         AS "Post ID",
    post.title                                      AS "Title",
    post.created_date                               AS "Timestamp",
    
    post.user_id                                    AS "User ID",
    LOWER(UDP.email)                                AS "Email",
    UDP.firstname                                   AS "First Name",
    UDP.lastname                                    AS "Last Name",
    CAST(UDP.level AS VARCHAR)                      AS "Level",
    COALESCE(UDP.country, 'Unspecified')            AS "User's Country",
    COALESCE(UDP.department, 'Unspecified')         AS "Department",

    post.question_1_value                           AS "Context",
    post.question_2_value                           AS "Data",
    post.question_3_value                           AS "Growth",
    post.question_4_value                           AS "Todo",
    
    COALESCE(tag_brand.label, 'Unspecified')        AS "Brand",
    COALESCE(tag_strategy.label, 'Unspecified')     AS "Strategy",
    COALESCE(tag_opportunity.label, 'Unspecified')  AS "Opportunity",
    COALESCE(tag_outcome.label, 'Unspecified')      AS "Outcome",
    COALESCE(tag_ability.label, 'Unspecified')      AS "Ability",
    COALESCE(tag_category.label, 'Unspecified')     AS "Category",
    COALESCE(tag_location.label, 'Unspecified')     AS "Location",
    
    COALESCE(tag_country.label, 'Unspecified')      AS "Geo Region",
    COALESCE(tag_continent.label, 'Unspecified')    AS "Continent",

    post.status                                     AS "Status"
FROM post
    LEFT JOIN tag_brand ON post.brand_id = tag_brand.id
    LEFT JOIN tag_strategy ON post.strategy_id = tag_strategy.id
    LEFT JOIN tag_opportunity ON post.opportunity_id = tag_opportunity.id
    LEFT JOIN tag_outcome ON post.outcome_id = tag_outcome.id
    LEFT JOIN tag_ability ON post.ability_id = tag_ability.id
    LEFT JOIN tag_category ON post.category_id = tag_category.id
    LEFT JOIN tag_location ON post.location_id = tag_location.id
    LEFT JOIN tag_country ON post.country_id = tag_country.id
    LEFT JOIN tag_continent ON tag_country.continent_id = tag_continent.id
    LEFT JOIN users_detail_with_preferences UDP ON post.user_id = UDP.id
-- WHERE 
--     post.status IN ('PUBLISHED', 'PENDING_VALIDATION')