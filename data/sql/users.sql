--select * 
--from user_account

SELECT -- Capitalize
    user_account.*,   -- Put the columns in a different line and indent
    tag_country.label
FROM
    user_account 
    JOIN tag_country -- Do not give your tables aliases if not necessary, and if you do, give them descriptive names
        ON user_account.country = tag_country.id
WHERE
    email LIKE '%.com%'