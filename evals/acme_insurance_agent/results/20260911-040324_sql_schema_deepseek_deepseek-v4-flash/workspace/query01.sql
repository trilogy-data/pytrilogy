-- Total premium paid on each policy: join Premium (a subtype of Policy_Amount)
-- back to its Policy_Amount row, then sum the premium amounts per policy.
SELECT pol.Policy_Number,
       SUM(pa.Policy_Amount) AS total_premium
FROM Premium pr
JOIN Policy_Amount pa USING (Policy_Amount_Identifier)
JOIN Policy pol ON pol.Policy_Identifier = pa.Policy_Identifier
GROUP BY pol.Policy_Number
ORDER BY pol.Policy_Number;
