SELECT
    apr.Party_Identifier AS policy_holder_id,
    COUNT(DISTINCT apr.Agreement_Identifier) AS policy_count
FROM Agreement_Party_Role apr
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
