SELECT apr.Party_Identifier AS Party_Identifier,
       SUM(pa.Policy_Amount) AS Total_Premium
FROM Premium pr
JOIN Policy_Amount pa
  ON pa.Policy_Amount_Identifier = pr.Policy_Amount_Identifier
JOIN Policy pol
  ON pol.Policy_Identifier = pa.Policy_Identifier
JOIN Agreement_Party_Role apr
  ON apr.Agreement_Identifier = pol.Policy_Identifier
 AND apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
