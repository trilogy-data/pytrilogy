SELECT apr.Party_Identifier, COUNT(*) AS policies_sold
FROM Agreement_Party_Role apr
JOIN Policy p ON p.Policy_Identifier = apr.Agreement_Identifier
WHERE apr.Party_Role_Code = 'AG'
GROUP BY apr.Party_Identifier
