SELECT
    p.Policy_Number,
    COUNT(DISTINCT c.Claim_Identifier) AS Claim_Count
FROM Policy p
JOIN Policy_Coverage_Detail pcd
    ON pcd.Policy_Identifier = p.Policy_Identifier
JOIN Claim_Coverage cc
    ON cc.Policy_Coverage_Detail_Identifier = pcd.Policy_Coverage_Detail_Identifier
JOIN Claim c
    ON c.Claim_Identifier = cc.Claim_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
