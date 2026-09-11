SELECT apr.Party_Identifier AS Party_Identifier,
       COUNT(DISTINCT p.Policy_Identifier) AS num_policies
FROM Agreement_Party_Role apr
JOIN Policy p ON p.Policy_Identifier = apr.Agreement_Identifier
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;