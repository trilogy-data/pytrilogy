SELECT SUM(pa.Policy_Amount) AS total_premiums_paid
FROM Premium pr
JOIN Policy_Amount pa
  ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier;
