SELECT apr.Party_Identifier AS agent_id,
       COUNT(DISTINCT p.Policy_Identifier) AS policy_count
FROM Agreement_Party_Role apr
JOIN Policy p
  ON p.Policy_Identifier = apr.Agreement_Identifier
WHERE apr.Party_Role_Code = 'AG'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
