SELECT
    apr.Party_Identifier AS Party_Identifier,
    COUNT(DISTINCT apr.Agreement_Identifier) AS Policy_Count
FROM Agreement_Party_Role AS apr
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
