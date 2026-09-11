SELECT SUM(pa.Policy_Amount) AS total_premiums_paid
FROM Premium p
JOIN Policy_Amount pa
  ON pa.Policy_Amount_Identifier = p.Policy_Amount_Identifier;
