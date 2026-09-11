-- Total loss amount = (loss payment amounts + loss reserve amounts) per claim number.
-- A Claim_Amount row is a loss payment/reserve if its identifier appears in the
-- Loss_Payment / Loss_Reserve link tables. Group by the claim's business number.
SELECT c.Company_Claim_Number                AS claim_number,
       SUM(ca.Claim_Amount)                  AS total_loss_amount
FROM Claim_Amount ca
JOIN Claim c
  ON c.Claim_Identifier = ca.Claim_Identifier
WHERE ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Payment)
   OR ca.Claim_Amount_Identifier IN (SELECT Claim_Amount_Identifier FROM Loss_Reserve)
GROUP BY c.Company_Claim_Number
ORDER BY c.Company_Claim_Number;
