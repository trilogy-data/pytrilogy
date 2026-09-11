SELECT
    Party_Identifier AS policy_holder_id,
    COUNT(DISTINCT Agreement_Identifier) AS policy_count
FROM Agreement_Party_Role
WHERE Party_Role_Code = 'PH'
GROUP BY Party_Identifier
ORDER BY Party_Identifier;
