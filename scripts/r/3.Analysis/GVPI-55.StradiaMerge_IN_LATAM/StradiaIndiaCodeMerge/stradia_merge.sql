
-- net listing
SELECT
DATE(created_at_first) as dia,
net_ad_counted as ad_counted,
COUNT(DISTINCT id)
FROM cars_in.ads
WHERE created_at_first>=  ' 2017-06-01'
AND net_ad_counted = 1
GROUP BY dia, ad_counted
;

-- all listings and then discriminate by net_ad_counted
-- can't find the device dimension
SELECT
DATE(created_at_first) as dia,
net_ad_counted as ad_counted,
COUNT(DISTINCT id) as listings
FROM cars_in.ads
WHERE created_at_first>=  ' 2017-06-01' AND created_at_first<= ' 2017-08-05'
GROUP BY dia, ad_counted
;


-- replies answers 2

SELECT
DATE(posted) as dia,
CASE 
	WHEN source= 'none' THEN 'desktop'
  WHEN source= 'apple' THEN 'ios'
  ELSE source
END AS device,
COUNT(DISTINCT id) as answers
FROM cars_in.answers
WHERE posted>=  ' 2017-04-02' AND posted<= ' 2017-08-05'
AND spam_status IN ('ok', 'probably_ok')
AND user_id = seller_id AND buyer_id = sender_id AND parent_id = 0
GROUP BY dia, device
;