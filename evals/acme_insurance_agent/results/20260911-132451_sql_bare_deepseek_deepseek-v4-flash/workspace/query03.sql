-- Total premium paid by each policy holder (Party_Role_Code = 'PH'),
-- summed across all of their policies. Policies are agreements here, so the
-- Policy's Agreement_Identifier is matched to the party-role agreement, and the
-- Premium table identifies which Policy_Amount rows are premium amounts.
SELECT
    apr.Party_Identifier AS party_id,
    SUM(pa.Policy_Amount) AS total_premium
FROM Agreement_Party_Role apr
JOIN Policy_Amount pa
    ON pa.Policy_Identifier = apr.Agreement_Identifier
JOIN Premium pr
    ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
WHERE apr.Party_Role_Code = 'PH'
GROUP BY apr.Party_Identifier
ORDER BY apr.Party_Identifier;
