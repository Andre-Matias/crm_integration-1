# olá Daniel,
# Gostava de perceber uma coisa relacionada com as leads nos paises onde 
#temos stockars - India e Argentina.
# 
# Consegues responder por favor:
#   1) Qual o total de chamadas que são geradas no OLX em anuncios do Stockars?
# (Se nao conseguires ver chamadas consegues ter acesso a cliques 
#no botao de chamar?)
# 2) Qual o total de emails (unicos) que são respondidos no OLX 
#em anuncios provenientes do Stockars?
# 3) Qual a distribuição destas leads por device?
# 4) Quantas respostas os dealers(sellers do stockars) são feitas no olx?
# 5) Quantas respostas os dealers(sellers do stockars) são feitas no stockars?
# 
# 
# Basicamente estou a tentar procurar:
# - Qual o meio preferencial que os buyers usam para contactar sellers no OLX
# - Qual o device que gera mais leads?
# - Qual o meio e device que os professional sellers usam para responder 
#aos buyers

# config ----------------------------------------------------------------------
options(scipen=999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

_by
# load libraries --------------------------------------------------------------
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("RPostgreSQL")
library("feather")
library("aws.s3")

# connect to Triton Silver ----------------------------------------------------

# drv <- dbDriver("PostgreSQL")
# 
# conDB <- 
#   dbConnect(
#     drv, 
#     host = dwTritonSilverDbHost,
#     port = dwTritonSilverDbPort,
#     dbname = "olxgroupbi",
#     user = myTritonUser,
#     password = myTritonPass
#   )

# get listings ----------------------------------------------------------------
# requestDB <- 
#   dbSendQuery(
#     conDB,
#           "
#           SELECT *FROM global_bi.fact_listings
#           WHERE country_sk = 'olx|asia|in'
#           AND listing_external_partner_code = 'crm'
#           AND category_sk LIKE 'olx|asia|in|5%'
#           AND date_posted_nk BETWEEN '2017-07-01' AND '2017-07-31';
#           "
#     )
# 
# dfRequestDB <- dbFetch(requestDB)
# dbClearResult(dbListResults(conDB)[[1]])
# 
# rawStockarsListingsInOlx <- dfRequestDB

# get replies -----------------------------------------------------------------
# requestDB <- 
#   dbSendQuery(
#     conDB,
#     "
#     SELECT * FROM global_bi.fact_replies
#     WHERE listing_sk IN
#     (
#       SELECT listing_sk
#       FROM global_bi.fact_listings
#       WHERE country_sk = 'olx|asia|in'
#       AND listing_external_partner_code = 'crm'
#       AND category_sk LIKE 'olx|asia|in|5%'
#       AND date_posted_nk BETWEEN '2017-07-01' AND '2017-07-31'
#     );
#       "
#   )
# 
# dfRequestDB <- dbFetch(requestDB)
# dbClearResult(dbListResults(conDB)[[1]])
# 
# rawStockarsListingsInOlxReplies <- dfRequestDB
# 
# 
# dbDisconnect(conDB)

rm("dfRequestDB")

# save raw data to aws s3 -----------------------------------------------------

# s3saveRDS(x = rawStockarsListingsInOlxReplies,
#           object = "rawStockarsListingsInOlxReplies.RDS",
#           bucket = "pyrates-data-ocean/GV-PI97")
# 
# s3saveRDS(x = rawStockarsListingsInOlx,
#           object = "rawStockarsListingsInOlx.RDS",
#           bucket = "pyrates-data-ocean/GV-PI97")

# load raw data from aws s3 ---------------------------------------------------

rawStockarsListingsInOlxReplies <- 
  s3readRDS(object = "rawStockarsListingsInOlxReplies.RDS", 
            bucket = "pyrates-data-ocean/GV-PI97"
            )

rawStockarsListingsInOlx <-
  s3readRDS(object = "rawStockarsListingsInOlx.RDS",
            bucket = "pyrates-data-ocean/GV-PI97"
            )

# connect to poseidon ---------------------------------------------------------
drv <- dbDriver("PostgreSQL")

conDB <- 
  dbConnect(
    drv, 
    host="bi-analytics.cnsuxis6zqxr.us-west-2.redshift.amazonaws.com",
    port = "5439",
    dbname = "analytics",
    user = userPoseidon,
    password = passPoseidon
  )

requestDB <- 
  dbSendQuery(
    conDB,
   "
   SELECT
   device_source_desc,
   reply_type_desc,
   device_source_group_desc,
   qtyLeads
   FROM
   (
   SELECT
   device_source_id,
   source_type_id,
   reply_type_id,
   COUNT(*) as qtyLeads
   FROM ods_naspers.ft_h_reply
   WHERE item_id IN (
   SELECT item_id
   FROM ods_naspers.ft_h_listing AS A
   WHERE A.country_id IN (32)
   AND A.category_l2_id = 378
   AND device_source_id = 27
   AND A.platform_id = 1 AND A.live_id = 1
   AND A.time_id BETWEEN '2017-07-01 00:00:00' AND '2017-07-31 00:00:00'
   )
   AND ( (reply_type_id in (2,5)) or(mail_sent_id=1 and source_type_id=4 and reply_type_id=1))
   GROUP BY 1, 2, 3
   )A
   LEFT JOIN master_data.str_reply_type R
   ON A.reply_type_id = R.reply_type_id
   LEFT JOIN master_data.str_device_source D
   ON A.device_source_id = D.device_source_id
   LEFT JOIN master_data.str_device_source_group G
   ON D.device_source_group_id = G.device_source_group_id
   ;
   "
  )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])
dbDisconnect(conDB)

rawStockarsLeadsOnPoseidon <- dfRequestDB



SELECT F.source, COUNT(*) QtyMessages FROM (
  SELECT *
    FROM
  (
    SELECT *
      FROM ods_naspers.ft_h_conversations
    WHERE item_id IN (
      SELECT item_id
      FROM ods_naspers.ft_h_listing AS A
      WHERE A.country_id IN (32)
      AND A.category_l2_id = 378
      AND device_source_id = 27
      AND A.platform_id = 1 AND A.live_id = 1
      AND A.time_id BETWEEN '2017-07-01 00:00:00' AND '2017-07-31 00:00:00'
    )
  ) A
  INNER JOIN (SELECT *
                FROM ods_naspers.ft_h_conversations
              WHERE country_id = 32) C
  ON A.item_id = C.item_id
  INNER JOIN (SELECT *
                FROM ods_naspers.ft_h_messages
              WHERE country_id = 32) M
  ON C.conversation_id = M.conversation_id
  WHERE C.seller_id = M.sender_id
)F
GROUP BY 1
;


