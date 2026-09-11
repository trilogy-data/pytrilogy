-- Average policy size = total premium amount / number of policies
SELECT SUM(pa.Policy_Amount) / COUNT(DISTINCT pol.Policy_Identifier) AS avg_policy_size
FROM Premium pr
JOIN Policy_Amount pa
  ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
JOIN Policy pol
  ON pa.Policy_Identifier = pol.Policy_Identifier;
