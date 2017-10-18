SELECT LAST_DAY(BP.starting_time), DP.name, COUNT(DISTINCT user_id)
FROM billing_periods BP
  INNER JOIN users U
  ON BP.user_id = U.id
  INNER JOIN dealer_packages DP
  ON BP.package_id = DP.id
  WHERE U.id IN (
    SELECT DISTINCT user_id FROM sap_invoices
    WHERE created_at BETWEEN '2017-05-01 00:00:00' AND '2017-06-30 23:59:59'
  )
  AND starting_time BETWEEN '2017-04-01 00:00:00' AND '2017-04-30 23:59:59'
  GROUP BY 1, 2

UNION ALL

SELECT LAST_DAY(BP.starting_time), DP.name, COUNT(DISTINCT user_id)
FROM billing_periods BP
  INNER JOIN users U
  ON BP.user_id = U.id
  INNER JOIN dealer_packages DP
  ON BP.package_id = DP.id
  WHERE U.id IN (
    SELECT DISTINCT user_id FROM sap_invoices
    WHERE created_at BETWEEN '2017-09-01 00:00:00' AND '2017-10-30 23:59:59'
  )
  AND starting_time BETWEEN '2017-09-01 00:00:00' AND '2017-09-30 23:59:59'
  GROUP BY 1, 2;
