SELECT po.Policy_Number,
       SUM(pa.Policy_Amount) AS total_premiums
FROM Premium p
JOIN Policy_Amount pa
  ON p.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
JOIN Policy po
  ON pa.Policy_Identifier = po.Policy_Identifier
GROUP BY po.Policy_Number
ORDER BY po.Policy_Number;
