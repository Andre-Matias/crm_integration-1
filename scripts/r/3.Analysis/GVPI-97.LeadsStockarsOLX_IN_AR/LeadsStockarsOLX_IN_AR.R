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
library("scales")
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

# get live listings in OLX India exported from Stockars -----------------------
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

# get replies to OLX India cars categories listings exported from Stockars-----
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

# connect to poseidon to get LATAM data (only leads) --------------------------
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
    conDB, paste(
          "SELECT F.source, COUNT(*) QtyMessages FROM (",
          "SELECT *",
          "FROM",
          "(",
          "SELECT *",
          "FROM ods_naspers.ft_h_conversations",
          "WHERE item_id IN (",
          "SELECT item_id",
          "FROM ods_naspers.ft_h_listing AS A",
          "WHERE A.country_id IN (32)",
          "AND A.category_l2_id = 378",
          "AND device_source_id = 27",
          "AND A.platform_id = 1 AND A.live_id = 1",
          "AND A.time_id BETWEEN '2017-07-01 00:00:00' AND '2017-07-31 00:00:00'",
          ")",
          ") A",
          "INNER JOIN (SELECT *",
          "    FROM ods_naspers.ft_h_conversations",
          "  WHERE country_id = 32) C",
          "ON A.item_id = C.item_id",
          "INNER JOIN (SELECT *",
          "    FROM ods_naspers.ft_h_messages",
          "  WHERE country_id = 32) M",
          "ON C.conversation_id = M.conversation_id",
          "WHERE C.seller_id = M.sender_id",
          ")F",
          "WHERE message_text != 'este producto ya no se encuentra disponible.'",
          "GROUP BY 1;"
  )
  )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])
dbDisconnect(conDB)

rawStockarsRepliesFromSellers<- dfRequestDB


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
    conDB, paste(
      "SELECT F.source, COUNT(*) QtyMessages FROM (",
      "SELECT *",
      "FROM",
      "(",
      "SELECT *",
      "FROM ods_naspers.ft_h_conversations",
      "WHERE item_id IN (",
      "SELECT item_id",
      "FROM ods_naspers.ft_h_listing AS A",
      "WHERE A.country_id IN (32)",
      "AND A.category_l2_id = 378",
      "AND device_source_id = 27",
      "AND A.platform_id = 1 AND A.live_id = 1",
      "AND A.time_id BETWEEN '2017-07-01 00:00:00' AND '2017-07-31 00:00:00'",
      ")",
      ") A",
      "INNER JOIN (SELECT *",
      "    FROM ods_naspers.ft_h_conversations",
      "  WHERE country_id = 32) C",
      "ON A.item_id = C.item_id",
      "INNER JOIN (SELECT *",
      "    FROM ods_naspers.ft_h_messages",
      "  WHERE country_id = 32) M",
      "ON C.conversation_id = M.conversation_id",
      "WHERE C.seller_id != M.sender_id",
      ")F",
      "WHERE message_text != 'este producto ya no se encuentra disponible.'",
      "GROUP BY 1;"
    )
  )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])
dbDisconnect(conDB)

rawStockarsRepliesFromBuyers<- dfRequestDB


# OLX India replies type ------------------------------------------------------

  dfOlxRepliesType <-
    rawStockarsListingsInOlxReplies %>%
    group_by(action_sk) %>%
    summarise (
      qtyLeadsByType = sum(n())
    ) %>%
    mutate(
      perLeadsByType = percent(round(qtyLeadsByType / sum(qtyLeadsByType),2))
    )
    
dfOlxRepliesByDevice <-
  rawStockarsListingsInOlxReplies %>%
  group_by(reply_channel_sk) %>%
  summarise (
    qtyLeadsByDevice = sum(n())
  ) %>%
  mutate(
    perLeadsByDevice =
      percent(round(qtyLeadsByDevice / sum(qtyLeadsByDevice),2))
  )

dfOlxRepliesByDeviceAndType <-
  rawStockarsListingsInOlxReplies %>%
  group_by(action_sk, reply_channel_sk) %>%
  summarise (
    qtyLeadsByDeviceAndType = sum(n())
  ) %>%
  group_by()%>%
  mutate(
    perLeadsByDeviceAndType = 
      percent(round(qtyLeadsByDeviceAndType / sum(qtyLeadsByDeviceAndType),2))
  )

# OLX Argentina replies type ----------------------------------------------------

dfOlxRepliesTypeArgentina <-
  rawStockarsLeadsOnPoseidon %>%
  group_by(reply_type_desc)  %>%
  summarise (
    qtyLeadsByType = sum(qtyleads)
  ) %>%
  mutate(
    perLeadsByType = percent(round(qtyLeadsByType / sum(qtyLeadsByType),2))
  )

dfOlxRepliesByDeviceArgentina <-
  rawStockarsLeadsOnPoseidon %>%
  group_by(device_source_desc) %>%
  summarise (
    qtyLeadsByDevice = sum(qtyleads)
  ) %>%
  mutate(
    perLeadsByDevice =
      percent(round(qtyLeadsByDevice / sum(qtyLeadsByDevice),2))
  )

dfOlxRepliesByDeviceAndTypeArgetina <-
  rawStockarsLeadsOnPoseidon %>%
  group_by(reply_type_desc, device_source_desc) %>%
  summarise (
    qtyLeadsByDeviceAndType = sum(qtyleads)
  ) %>%
  group_by()%>%
  mutate(
    perLeadsByDeviceAndType = 
      percent(round(qtyLeadsByDeviceAndType / sum(qtyLeadsByDeviceAndType),2))
  )

# OLX Argentina dealer's responses --------------------------------------------

dfOlxArgentinaDealersResponses <-
  rawStockarsRepliesFromSellers %>%
  filter(!is.na(source)) %>%
  mutate(perMessages = 
           percent(round(qtymessages /sum(qtymessages),2)))


# OLX Argentina buyers's responses --------------------------------------------
dfOlxArgentinaBuyersResponses <-
  rawStockarsRepliesFromBuyers %>%
  filter(!is.na(source)) %>%
  mutate(perMessages = 
           percent(round(qtymessages /sum(qtymessages),2)))

# connect to stockars ---------------------------------------------------------

dbUsername <- "biuser"
dbPassword <- biUserPassword
dbHost <- "172.61.11.31"
dbPort <- "3306"
dbName <- "crm_cars_ar"

sshUser <- "biuser"
sshHost <- "52.33.194.191"
sshPort <- "10022"

dbLocalPort <- 10003
dbLocalHost <- "127.0.0.1"

system("killall ssh", wait=FALSE)

cmdSSH <-
  paste0(
    "ssh -i", " ",  sshKeyPath, " ", sshUser, "@", sshHost, " ", "-p", " ", 
    sshPort, " ", "-L", " ",  dbLocalPort, ":", dbHost ,":", dbPort," ", "-N"
  )

system(cmdSSH, wait=FALSE)

Sys.sleep(5)
conDB <-  dbConnect(RMySQL::MySQL(), username = dbUsername,
                    password = dbPassword , host = dbLocalHost,
                    port = dbLocalPort , dbname = dbName)

sqlCmd <- 
  "SELECT * FROM message A 
  LEFT JOIN
  (SELECT id_thread, partner_name, id_product FROM message_thread) B
  ON A.id_thread = B.id_thread
  WHERE 
  message_date >= '2017-07-01 00:00:00' 
  AND message_date < '2017-08-01 00:00:00';
  "

dfSqlCmd <- dbGetQuery(conDB,sqlCmd)
rawStockarsMessages <- as.data.frame(dfSqlCmd)


dfSentMessagesFromStockars <-
  rawStockarsMessages[ ,c("message_date", "direction", "partner_name")] %>%
  filter(direction == 0, partner_name == 'olx') %>%
  mutate(MessageDay = as.Date(message_date)) %>%
  group_by() %>%
  summarise(qtyMessagesSent = sum(!is.na(MessageDay)))

dfOlxArgentinaDealersResponsesTotal <-  sum(dfOlxArgentinaDealersResponses$qtymessages)