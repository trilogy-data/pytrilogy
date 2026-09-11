SELECT SUM(pa.Policy_Amount) AS Total_Premiums_Paid
FROM Premium p
JOIN Policy_Amount pa
  ON pa.Policy_Amount_Identifier = p.Policy_Amount_Identifier;
