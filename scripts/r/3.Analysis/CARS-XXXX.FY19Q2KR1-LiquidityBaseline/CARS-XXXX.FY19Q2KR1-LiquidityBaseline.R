 # FY19 Q2 OKR - Increase Liquidity of Platinum Package
 # 
 # Liquidity Metric Definition: = Liquidy Listings / Total Listings(w/ + w/o Liquidity)
 # 
 #   Liquidity listing: Listing with 1 reply(1 unique message OR unique call OR 1 unique SMS ) on the first 7 days after create data.

# GET listings with package classification

# SELECT ad_id, created_at_first, name
# FROM (
#   SELECT ad_id, A.user_id, category_id, created_at_first, package_id, starting_time, ending_time
#   FROM
#   (SELECT
#     id as ad_id,
#     user_id,
#     category_id,
#     created_at_first
#     FROM ads
#     WHERE
#     created_at_first >= '2018-06-01 00:00:00' AND created_at_first < '2018-07-01 00:00:00'
#     AND category_id = 29
#     AND user_id IN (SELECT id
#                     FROM users
#                     WHERE is_business = 1)
#   ) A
#   INNER JOIN billing_periods BP
#   ON A.user_id = BP.user_id
#   AND A.created_at_first >= BP.starting_time
#   AND A.created_at_first < BP.ending_time
# )Z
# LEFT JOIN
# (SELECT id, name FROM dealer_packages) DP
# ON Z.package_id = DP.id
# ;



# # getting messages
# 
# SELECT Z.ad_id, created_at_first, name, M.posted, M.buyer_id
# FROM (
#   SELECT ad_id, A.user_id, category_id, created_at_first, package_id, starting_time, ending_time
#   FROM
#   (SELECT
#     id as ad_id,
#     user_id,
#     category_id,
#     created_at_first
#     FROM ads
#     WHERE
#     created_at_first >= '2018-06-01 00:00:00' AND created_at_first < '2018-07-01 00:00:00'
#     AND category_id = 29
#     AND user_id IN (SELECT id
#                     FROM users
#                     WHERE is_business = 1)
#   ) A
#   INNER JOIN billing_periods BP
#   ON A.user_id = BP.user_id
#   AND A.created_at_first >= BP.starting_time
#   AND A.created_at_first < BP.ending_time
# )Z
# LEFT JOIN
# (SELECT id, name FROM dealer_packages) DP
# ON Z.package_id = DP.id
# INNER JOIN (
#   SELECT
#   id,
#   ad_id,
#   buyer_id,
#   posted
#   FROM answers
#   WHERE 1 = 1
#   AND parent_id = 0
#   AND user_id = seller_id
#   AND buyer_id = sender_id
#   AND buyer_id != seller_id
# )M
# ON Z.ad_id = M.ad_id


# gettings calls and SMS from 

# SELECT *
#   FROM main.aggregates.ad_traffic_verticals
# WHERE stream IN ('v-otomoto-web', 'v-otomoto-android', 'v-otomoto-ios')
# AND server_date_day >= '2018-06-01' AND server_date_day <= '2018-07-07';