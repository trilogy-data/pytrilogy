SELECT apr.Party_Identifier AS party_id, SUM(pa.Policy_Amount) AS total_premium
FROM Premium p
JOIN Policy_Amount pa ON pa.Policy_Amount_Identifier = p.Policy_Amount_Identifier
JOIN Agreement_Party_Role apr ON apr.Agreement_Identifier = pa.Policy_Identifier
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
