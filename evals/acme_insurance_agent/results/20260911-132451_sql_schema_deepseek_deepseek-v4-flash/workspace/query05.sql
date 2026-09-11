SELECT
    c.Company_Claim_Number,
    SUM(
        CASE
            WHEN lp.Claim_Amount_Identifier IS NOT NULL
              OR lr.Claim_Amount_Identifier IS NOT NULL
            THEN ca.Claim_Amount
            ELSE 0
        END
    ) AS loss_amount
FROM Claim c
JOIN Claim_Amount ca
    ON ca.Claim_Identifier = c.Claim_Identifier
LEFT JOIN Loss_Payment lp
    ON lp.Claim_Amount_Identifier = ca.Claim_Amount_Identifier
LEFT JOIN Loss_Reserve lr
    ON lr.Claim_Amount_Identifier = ca.Claim_Amount_Identifier
GROUP BY c.Claim_Identifier, c.Company_Claim_Number
ORDER BY c.Company_Claim_Number;
