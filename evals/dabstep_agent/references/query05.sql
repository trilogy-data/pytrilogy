-- DABstep task 5 (easy): Which issuing country has the highest number of
-- transactions? Published answer: NL.
SELECT issuing_country
FROM payments
GROUP BY issuing_country
ORDER BY count(*) DESC
LIMIT 1
