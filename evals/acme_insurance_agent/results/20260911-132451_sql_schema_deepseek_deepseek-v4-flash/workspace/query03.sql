SELECT apr.Party_Identifier, SUM(pa.Policy_Amount) AS total_premium
FROM Premium pr
JOIN Policy_Amount pa ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
JOIN Policy p ON pa.Policy_Identifier = p.Policy_Identifier
JOIN Agreement_Party_Role apr
  ON apr.Agreement_Identifier = p.Policy_Identifier
 AND apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
