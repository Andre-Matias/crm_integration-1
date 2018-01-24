/*
Get all the ads exported from OTOMOTO to OLX.PL
since 2017-07-01 until 2018-01-01 (last 6 complete months)
*/
WITH
ads AS
  (
      SELECT
        A.id,
        A.private_business,
        D.name_pl
      FROM
        (SELECT *
         FROM db_atlas.olxpl_ads
         WHERE external_partner_code = 'otomoto_pl_form'
               AND created_at_first BETWEEN '2017-07-01 00:00:00'
               AND '2018-01-01 00:00:00'
                and coalesce(operation_type,'insert') != 'delete'
        ) A
        INNER JOIN (SELECT
                      id AS cat_id,
                      parent_id
                    FROM db_atlas.olxpl_categories) C ON A.category_id = C.cat_id
        INNER JOIN (SELECT
                      id AS par_id,
                      name_pl
                    FROM db_atlas.olxpl_categories) D ON C.parent_id = D.par_id
  )

/*
Get the quantity of VAS purchased in OLX for the ads that were exported from OTOMOTO to OLX
*/

SELECT type, count(*)
  FROM db_atlas.olxpl_payment_basket OPB
INNER JOIN /* join to get the names of the VAS */
    (SELECT id, name_pl, description, "type" FROM db_atlas.olxpl_paidads_indexes
    WHERE coalesce(operation_type,'insert') != 'delete')PIP
      ON OPB.index_id = PIP.id
  INNER JOIN /* join to check if the session was finished */
    (SELECT id, "status" FROM db_atlas.olxpl_payment_session
    WHERE coalesce(operation_type,'insert') != 'delete'
    )OPS
      ON OPS.id = OPB.session_id
WHERE OPB.ad_id IN (SELECT id FROM ads) /* only ads that were exported from OTOMOTO */
  AND OPS.status = 'finished' /* only finished payment sessions */
      and coalesce(operation_type,'insert') != 'delete'
GROUP BY 1;
