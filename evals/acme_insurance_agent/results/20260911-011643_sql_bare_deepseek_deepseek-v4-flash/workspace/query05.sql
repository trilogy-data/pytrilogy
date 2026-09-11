SELECT
    c.Company_Claim_Number AS claim_number,
    SUM(ca.Claim_Amount)   AS total_loss_amount
FROM Claim_Amount ca
JOIN Claim c
    ON c.Claim_Identifier = ca.Claim_Identifier
WHERE ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Payment)
   OR ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Reserve)
GROUP BY c.Company_Claim_Number
ORDER BY c.Company_Claim_Number
