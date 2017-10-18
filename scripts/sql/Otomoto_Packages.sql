SELECT LAST_DAY(BP.ending_time)Month, DP.name, COUNT(DISTINCT BP.user_id) qtyPackages
FROM billing_periods BP
  INNER JOIN users U
  ON BP.user_id = U.id
  INNER JOIN dealer_packages DP
  ON BP.package_id = DP.id
  INNER JOIN sap_invoices fi
  ON U.id = fi.user_id
  WHERE fi.created_at
  BETWEEN DATE_ADD(BP.ending_time, INTERVAL 0 DAY) AND DATE_ADD(BP.ending_time, INTERVAL 30 DAY)
GROUP BY 1,2;
