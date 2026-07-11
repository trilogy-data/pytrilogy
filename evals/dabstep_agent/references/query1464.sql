-- DABstep task 1464 (hard): What is the fee ID or IDs that apply to
-- account_type = R and aci = B? Published answer: 416 ids (validated exact).
-- Empty child-table set = rule unconstrained on that dimension.
SELECT ID AS fee_id
FROM fees f
WHERE (NOT EXISTS (SELECT 1 FROM fee_account_types a WHERE a.fee_id = f.ID)
       OR EXISTS (SELECT 1 FROM fee_account_types a
                  WHERE a.fee_id = f.ID AND a.account_type = 'R'))
  AND (NOT EXISTS (SELECT 1 FROM fee_acis c WHERE c.fee_id = f.ID)
       OR EXISTS (SELECT 1 FROM fee_acis c
                  WHERE c.fee_id = f.ID AND c.aci = 'B'))
ORDER BY ID
