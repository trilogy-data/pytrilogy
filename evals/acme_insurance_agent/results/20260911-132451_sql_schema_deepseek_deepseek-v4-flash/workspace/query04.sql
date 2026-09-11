SELECT
    ap.Party_Identifier AS Party_Identifier,
    COUNT(DISTINCT p.Policy_Identifier) AS Number_Of_Policies
FROM Agreement_Party_Role AS ap
JOIN Policy AS p
    ON p.Policy_Identifier = ap.Agreement_Identifier
WHERE ap.Party_Role_Code = 'AG'
GROUP BY ap.Party_Identifier
ORDER BY ap.Party_Identifier;
