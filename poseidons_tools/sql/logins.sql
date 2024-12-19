WITH first_login AS (
    SELECT 
        userid,
        MIN(date_time)  AS "First Login"
    FROM metabase_login_report
    WHERE action <> 'LOGINFAIL'
    GROUP BY userid
)

SELECT
    MLR.userid                                      AS "User ID",
    TRIM(CONCAT(UDP.firstname, ' ', UDP.lastname))  AS "Full Name",
    LOWER(MLR.email)                                AS "Email",
    MLR.date_time                                   AS "Timestamp",
    "First Login",
    CAST(MLR.userlevel AS VARCHAR)                  AS "Level",
    COALESCE(TCO.label, 'Unspecified')              AS "Geo Region",
    COALESCE(TCN.label, 'Unspecified')              AS "Continent",
    COALESCE(UDP.department, 'Unspecified')         AS "Department"
FROM metabase_login_report MLR
    LEFT JOIN users_detail_with_preferences UDP
        ON MLR.userid = UDP.id
    LEFT JOIN user_account UAC 
        ON MLR.userid = UAC.id
    LEFT JOIN tag_country TCO 
        ON UAC.country = TCO.id
    LEFT JOIN tag_continent TCN 
        ON TCO.continent_id = TCN.id
    LEFT JOIN first_login FLG 
        ON MLR.userid = FLG.userid
WHERE 
    MLR.action <> 'LOGINFAIL'
    AND MLR.userid IS NOT NULL