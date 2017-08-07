select 
  TO_DATE(date, 'YYYY-MM') as time, 
  u.is_business, 
  count(distinct p.id_user) as cnt
FROM 
  (SELECT t.* 
   FROM livesync.verticals_paidads_user_payments t 
   WHERE livesync_dbname = 'otomotopl'
  ) AS p
INNER JOIN 
  (SELECT t.* 
   FROM livesync.verticals_users t 
   WHERE livesync_dbname = 'otomotopl'
  ) AS u
  ON u.id = p.id_user
WHERE
date >= '2015-04-01'
AND is_removed_from_invoice = 0
AND is_invalid_item = 0
AND payment_provider = 'postpay'
AND price<0 
AND NOT (
  u.email LIKE '%@sunfra.%' 
  OR u.email LIKE '%@olx.%' 
  OR u.email LIKE '%@tablica.%' 
  OR u.email LIKE '%@fixeads.%' 
  OR u.email LIKE '%@otomoto.%' 
  OR u.email LIKE '%@otodom.%' 
  OR u.email LIKE '%@slando.%')
AND u.is_business=1
GROUP BY 1,2
ORDER BY 1;
