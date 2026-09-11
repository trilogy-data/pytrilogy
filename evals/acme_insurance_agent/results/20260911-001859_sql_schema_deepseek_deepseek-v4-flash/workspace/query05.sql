SELECT c.Company_Claim_Number AS Claim_Number,
       SUM(ca.Claim_Amount)   AS Total_Loss_Amount
FROM Claim c
JOIN Claim_Amount ca
  ON ca.Claim_Identifier = c.Claim_Identifier
JOIN (
    SELECT Claim_Amount_Identifier FROM Loss_Payment
    UNION
    SELECT Claim_Amount_Identifier FROM Loss_Reserve
) loss ON loss.Claim_Amount_Identifier = ca.Claim_Amount_Identifier
GROUP BY c.Company_Claim_Number
ORDER BY c.Company_Claim_Number;
