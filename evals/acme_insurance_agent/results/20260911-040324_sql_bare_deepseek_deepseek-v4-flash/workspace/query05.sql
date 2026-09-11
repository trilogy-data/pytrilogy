SELECT
    c.Company_Claim_Number,
    COALESCE(lp.loss_paid, 0) + COALESCE(lr.loss_reserved, 0) AS loss_amount
FROM Claim AS c
LEFT JOIN (
    SELECT ca.Claim_Identifier, SUM(ca.Claim_Amount) AS loss_paid
    FROM Loss_Payment AS lp
    JOIN Claim_Amount AS ca
      ON ca.Claim_Amount_Identifier = lp.Claim_Amount_Identifier
    GROUP BY ca.Claim_Identifier
) AS lp
  ON lp.Claim_Identifier = c.Claim_Identifier
LEFT JOIN (
    SELECT ca.Claim_Identifier, SUM(ca.Claim_Amount) AS loss_reserved
    FROM Loss_Reserve AS lr
    JOIN Claim_Amount AS ca
      ON ca.Claim_Amount_Identifier = lr.Claim_Amount_Identifier
    GROUP BY ca.Claim_Identifier
) AS lr
  ON lr.Claim_Identifier = c.Claim_Identifier
ORDER BY c.Company_Claim_Number;
