SELECT
    apr.Party_Identifier AS party_id,
    COUNT(DISTINCT p.Policy_Identifier) AS policy_count
FROM Agreement_Party_Role AS apr
JOIN Policy AS p
    ON p.Policy_Identifier = apr.Agreement_Identifier
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
