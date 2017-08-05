SELECT '2017-07' as month, COUNT(DISTINCT buyer) as cnt
FROM crm_cars_in.`package_subscription` ps
INNER JOIN crm_cars_in.`package` p ON ps.package_id = p.id
INNER JOIN crm_cars_in.`user` u ON u.`id_user` = ps.`buyer`
WHERE ps.STATUS = 1
AND (LOWER(p.`name`) LIKE '%retail%' /*OR LOWER(p.`name`) LIKE '%corporate%' )*/
AND (CAST(SUBSTR(p.`name`,
                  LOCATE('Rs_',p.`name` )+3,
                  LOCATE( '_',p.name, LOCATE('Rs_',p.`name` )+3)-LOCATE('Rs_',p.`name` ) -3 ) AS UNSIGNED) > 0
OR p.`name` LIKE '%Corporate Stockars : Paid package'
)
AND NOT( u.`email` LIKE '%olx.com'
         OR u.`email` LIKE '%sunfra.com'
         OR u.`email` LIKE '%sunfra.in'
         OR u.`email` LIKE '%fixeads.com'
         OR u.`email` LIKE '%dispostable.com'
)
AND NOT(ps.end_date <= '2017-07-30 00:00:00'
        OR ps.start_date >= '2017-07-31 23:59:59'))
GROUP BY 1;