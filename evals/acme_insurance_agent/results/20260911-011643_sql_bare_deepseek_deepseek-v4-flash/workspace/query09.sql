-- Average policy size = total premium / number of policies
-- Premium rows are the Policy_Amount rows flagged by the Premium table.
SELECT
    (SELECT SUM(pa.Policy_Amount)
       FROM Policy_Amount pa
       JOIN Premium pr ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier)
    / (SELECT COUNT(*) FROM Policy) AS avg_policy_size;
