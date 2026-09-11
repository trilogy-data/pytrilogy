SELECT
    p.Policy_Number,
    COUNT(DISTINCT c.Claim_Identifier) AS Claim_Count
FROM Claim c
JOIN Claim_Coverage cc
    ON cc.Claim_Identifier = c.Claim_Identifier
JOIN Policy_Coverage_Detail pcd
    ON pcd.Policy_Coverage_Detail_Identifier = cc.Policy_Coverage_Detail_Identifier
   AND pcd.Effective_Date = cc.Effective_Date
JOIN Policy p
    ON p.Policy_Identifier = pcd.Policy_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
