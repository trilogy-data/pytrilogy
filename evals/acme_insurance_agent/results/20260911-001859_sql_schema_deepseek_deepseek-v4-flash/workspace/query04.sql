SELECT apr.Party_Identifier AS Agent_Id,
       COUNT(DISTINCT p.Policy_Identifier) AS Policy_Count
FROM Policy p
JOIN Agreement_Party_Role apr
  ON apr.Agreement_Identifier = p.Policy_Identifier
WHERE apr.Party_Role_Code = 'AG'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
