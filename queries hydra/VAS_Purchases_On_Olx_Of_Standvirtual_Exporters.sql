/*
Get all the ads exported from STANDVIRTUAL to OLX.PL
since 2017-07-01 until 2018-01-01 (last 6 complete months)
*/

WITH
ads AS
  (
      SELECT DISTINCT
        A.id,
        A.private_business,
        A.category_id,
        D.name_pt
      FROM
        (SELECT *
         FROM db_atlas.olxpt_ads
         WHERE external_partner_code = 'standvirtual'
               AND created_at_first BETWEEN '2017-07-01 00:00:00'
               AND '2018-01-01 00:00:00'
                and coalesce(operation_type,'insert') != 'delete'
          AND category_id != 377 /* exclude parts */
        ) A
        INNER JOIN (SELECT
                      id AS cat_id,
                      parent_id
                    FROM db_atlas.olxpt_categories) C ON A.category_id = C.cat_id
        INNER JOIN (SELECT
                      id AS par_id,
                      name_pt
                    FROM db_atlas.olxpt_categories) D ON C.parent_id = D.par_id
  )
  SELECT COUNT(DISTINCT id) FROM ads;
/*
Get the quantity of VAS purchased in OLX for the ads that were exported from STANDVIRTUAL to OLX
*/

SELECT type, count(DISTINCT ad_id)qtyVasUniqueAd, COUNT(*)qtyTotalVasAquired
  FROM db_atlas.olxpt_payment_basket OPB
INNER JOIN /* join to get the names of the VAS */
    (SELECT id, name_pt, description, "type" FROM db_atlas.olxpt_paidads_indexes
    WHERE coalesce(operation_type,'insert') != 'delete')PIP
      ON OPB.index_id = PIP.id
  INNER JOIN /* join to check if the session was finished */
    (SELECT id, "status" FROM db_atlas.olxpt_payment_session
    WHERE coalesce(operation_type,'insert') != 'delete'
    )OPS
      ON OPS.id = OPB.session_id
WHERE OPB.ad_id IN (SELECT id FROM ads) /* only ads that were exported from STANDVIRTUAL */
  AND OPS.status = 'finished' /* only finished payment sessions */
      and coalesce(operation_type,'insert') != 'delete'
GROUP BY 1;
