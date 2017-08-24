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


# load libraries --------------------------------------------------------------
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("RPostgreSQL")
library("feather")
library("aws.s3")


bucketlist()

# connect to Triton Silver ----------------------------------------------------

drv <- dbDriver("PostgreSQL")

conDB <- 
  dbConnect(
    drv, 
    host = dwTritonSilverDbHost,
    port = dwTritonSilverDbPort,
    dbname = "olxgroupbi",
    user = myTritonUser,
    password = myTritonPass
  )

# get listings ----------------------------------------------------------------
requestDB <- 
  dbSendQuery(
    conDB,
          "
          SELECT *FROM global_bi.fact_listings
          WHERE country_sk = 'olx|asia|in'
          AND listing_external_partner_code = 'crm'
          AND category_sk LIKE 'olx|asia|in|5%'
          AND date_posted_nk BETWEEN '2017-07-01' AND '2017-07-31';
          "
    )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])

rawStockarsListingsInOlx <- dfRequestDB

# get replies -----------------------------------------------------------------
requestDB <- 
  dbSendQuery(
    conDB,
    "
    SELECT * FROM global_bi.fact_replies
    WHERE listing_sk IN
    (
      SELECT listing_sk
      FROM global_bi.fact_listings
      WHERE country_sk = 'olx|asia|in'
      AND listing_external_partner_code = 'crm'
      AND category_sk LIKE 'olx|asia|in|5%'
      AND date_posted_nk BETWEEN '2017-07-01' AND '2017-07-31'
    );
      "
  )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])

rawStockarsListingsInOlxReplies <- dfRequestDB


dbDisconnect(conDB)

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