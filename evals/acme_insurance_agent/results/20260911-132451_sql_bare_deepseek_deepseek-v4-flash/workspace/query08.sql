SELECT
    p.Policy_Number,
    COUNT(*) AS claim_count
FROM Claim AS c
JOIN (
    SELECT DISTINCT Insurable_Object_Identifier, Policy_Identifier
    FROM Policy_Coverage_Detail
) AS m
    ON m.Insurable_Object_Identifier = c.Insurable_Object_Identifier
JOIN Policy AS p
    ON p.Policy_Identifier = m.Policy_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
