SELECT p.Policy_Number,
       AVG(date_diff('day', c.Claim_Open_Date, c.Claim_Close_Date)) AS avg_days_to_settle
FROM Claim c
JOIN Claim_Coverage cc
  ON c.Claim_Identifier = cc.Claim_Identifier
JOIN Policy_Coverage_Detail pcd
  ON cc.Effective_Date = pcd.Effective_Date
 AND cc.Policy_Coverage_Detail_Identifier = pcd.Policy_Coverage_Detail_Identifier
JOIN Policy p
  ON pcd.Policy_Identifier = p.Policy_Identifier
WHERE c.Claim_Close_Date IS NOT NULL
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
