SELECT p.Policy_Number,
       AVG(c.Claim_Close_Date - c.Claim_Open_Date) AS avg_settle_days
FROM Claim c
JOIN Claim_Coverage cc
  ON cc.Claim_Identifier = c.Claim_Identifier
JOIN Policy_Coverage_Detail pcd
  ON pcd.Policy_Coverage_Detail_Identifier = cc.Policy_Coverage_Detail_Identifier
JOIN Policy p
  ON p.Policy_Identifier = pcd.Policy_Identifier
WHERE c.Claim_Close_Date IS NOT NULL
  AND c.Claim_Open_Date IS NOT NULL
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
