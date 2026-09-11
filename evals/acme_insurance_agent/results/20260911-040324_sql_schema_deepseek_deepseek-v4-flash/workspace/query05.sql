SELECT c.Company_Claim_Number,
       SUM(ca.Claim_Amount) AS Loss_Amount
FROM Claim c
JOIN Claim_Amount ca ON ca.Claim_Identifier = c.Claim_Identifier
WHERE ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Payment)
   OR ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Reserve)
GROUP BY c.Claim_Identifier, c.Company_Claim_Number
ORDER BY c.Company_Claim_Number;
