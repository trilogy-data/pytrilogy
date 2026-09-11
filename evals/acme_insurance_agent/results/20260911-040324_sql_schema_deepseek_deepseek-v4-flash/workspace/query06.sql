SELECT
    apr.Party_Identifier            AS Party_Identifier,
    COUNT(DISTINCT p.Policy_Identifier) AS Number_Of_Policies
FROM Policy p
JOIN Agreement_Party_Role apr
    ON apr.Agreement_Identifier = p.Policy_Identifier
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier
