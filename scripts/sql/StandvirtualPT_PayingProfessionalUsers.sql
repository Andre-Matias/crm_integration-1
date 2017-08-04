# StandvirtualPT - Paying Professional Users ----------------------------------
SELECT
  LAST_DAY(last_status_date) date,
  COUNT(DISTINCT user_id) pmul
FROM carspt.payment_session ps
INNER JOIN carspt.payment_basket pb ON ps.id=pb.session_id
INNER JOIN carspt.users u ON pb.user_id=u.id
INNER JOIN carspt.paidads_indexes pi ON pb.index_id=pi.id
INNER JOIN forbi.dim_product p ON pi.code=p.product_id
WHERE ps.status='finished'
  AND provider NOT IN('admin','volume')
  AND last_status_date>='2017-01-01'
  AND is_business=1
  AND is_revenue='Y'
  AND pi.code != 'expiredcredits'
GROUP BY 1;