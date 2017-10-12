WITH ads AS (
  SELECT
  A.livesync_dbname,
  A.id AS id_listing,
  C.name_en,
  A.created_at_first,
  (LEN(NVL(description, '')) < 1) AS blank_description,
  TRUNC(A.created_at_first) created_at_first_day
  FROM olxgroupbi.livesync.verticals_ads A
  INNER JOIN olxgroupbi.livesync.verticals_categories C
  ON A.category_id = C.id
  WHERE created_at_first BETWEEN '2017-06-01 00:00:00' AND '2017-09-29 00:00:00'
  AND A.livesync_dbname = 'carspt' AND C.livesync_dbname = 'carspt'
  AND name_en = 'Cars'
),
messages AS
(SELECT id, ad_id, posted
  FROM olxgroupbi.livesync.verticals_answers
  WHERE
  ad_id IN (SELECT DISTINCT id_listing FROM ads)
  AND livesync_dbname = 'carspt'
  AND parent_id = 0
  AND user_id = seller_id
  AND buyer_id = sender_id
  AND buyer_id != seller_id
),
ads_messages AS
(
  SELECT
  D.livesync_dbname,
  D.id_listing,
  D.blank_description,
  D.name_en,
  D.created_at_first_day,
  SUM(NVL2(C.posted, 1, 0)),
  CASE WHEN SUM(NVL2(C.posted, 1, 0)) >= 1
  THEN 1
  ELSE 0 END AS listing_with_liquidity
  FROM ads D
  LEFT JOIN
  (SELECT *
      FROM
    ads A
    INNER JOIN
    messages M
    ON A.id_listing = M.ad_id
    WHERE
    posted >= created_at_first
    AND posted < DATEADD(DAYS, 7, TRUNC(created_at_first))
  ) C
  ON D.id_listing = C.id_listing
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
livesync_dbname,
created_at_first_day,
name_en,
blank_description,
COUNT(DISTINCT id_listing) qtyListings,
SUM(listing_with_liquidity) qtyListingsWithLiquidity,
CAST(SUM(listing_with_liquidity) AS NUMERIC) / CAST(COUNT(DISTINCT id_listing) AS NUMERIC) perListingsWithLiquidity
FROM ads_messages
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4, 2
;