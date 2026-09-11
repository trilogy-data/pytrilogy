WITH claim_policy AS (
  SELECT DISTINCT
         c.Claim_Identifier,
         c.Claim_Open_Date,
         c.Claim_Close_Date,
         pcd.Policy_Identifier
  FROM Claim c
  JOIN Policy_Coverage_Detail pcd
    ON c.Insurable_Object_Identifier = pcd.Insurable_Object_Identifier
  WHERE c.Claim_Close_Date IS NOT NULL
)
SELECT p.Policy_Number,
       AVG(cp.Claim_Close_Date - cp.Claim_Open_Date) AS avg_days_to_settle
FROM claim_policy cp
JOIN Policy p
  ON cp.Policy_Identifier = p.Policy_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number
