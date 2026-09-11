SELECT
    apr.Party_Identifier AS agent_party_id,
    COUNT(*)             AS policies_sold
FROM Policy AS p
JOIN Agreement_Party_Role AS apr
    ON apr.Agreement_Identifier = p.Policy_Identifier
WHERE apr.Party_Role_Code = 'AG'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier
