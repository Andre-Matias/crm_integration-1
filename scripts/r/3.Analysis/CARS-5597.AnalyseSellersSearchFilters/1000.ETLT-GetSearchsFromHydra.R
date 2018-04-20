# load libraries --------------------------------------------------------------

library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("RMySQL")
library("RPostgreSQL")
library("tidyr")
library("scales")
library("ggplot2")
library("ggthemes")
library("fasttime")
library("forcats")
library("RColorBrewer")
library("gridExtra")
library("aws.s3")

# config ----------------------------------------------------------------------
options(scipen=999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# 
rangeDate <- as.character(seq(as.Date('2018-02-21'),as.Date('2018-02-26'),by = 1))

# connect to silver ---------------------------------------------------------
drv <- dbDriver("PostgreSQL")

hosts <- c("/h/v-otomoto-web", "/h/v-standvirtual-web", "/h/v-autovit-web")
for(host in hosts){
for(i in rangeDate){

  print (paste(host, "=>", i, "=> started at:", Sys.time()))

conDB <- 
  dbConnect(
    drv, 
    host= dwTritonSilverDbHost,
    port = dwTritonSilverDbPort,
    dbname = dwTritonSilverDbName,
    user = myTritonUser,
    password = myTritonPass
  )

sqlQuery <-
  "
  SELECT *
  FROM hydra.verticals_ninja_android_##partition##
  WHERE trackpage = 'listing'
  AND server_path = '##host##'
  AND user_id IS NOT NULL AND server_date_trunc = date_part(epoch, '##Date##')
  ;
  "

sqlQuery <- as.character(gsub("##Date##", i,  sqlQuery))
sqlQuery <- as.character(gsub("##host##", host,  sqlQuery))

sqlQuery <- as.character(gsub("##partition##", gsub("-", "", substr(i, 1, 7)), sqlQuery))

requestDB <- 
  dbSendQuery(
    conDB,
    sqlQuery
  )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])
dbDisconnect(conDB)

dfHydraResults <- dfRequestDB

rm("dfRequestDB")


dfHydraResults$extra <- gsub("\"\\[", "\\[", dfHydraResults$extra)
dfHydraResults$extra <- gsub("\\]\"", "\\]", dfHydraResults$extra)
dfHydraResults$extra <- lapply(dfHydraResults$extra, function(x) paste0(x, collapse=""))
dfHydraResults$a <- purrr::map(dfHydraResults$extra, jsonlite::validate)
dfHydraResults <- dfHydraResults[dfHydraResults$a == TRUE, ]

s3saveRDS(x = dfHydraResults,
          object = paste0("Results_", gsub("/h/", "", host), "_", i, ".RDS"),
          bucket = paste0("pyrates-data-ocean/CARS/CARS-5597/", gsub("/h/", "", host))
          )
}
}

