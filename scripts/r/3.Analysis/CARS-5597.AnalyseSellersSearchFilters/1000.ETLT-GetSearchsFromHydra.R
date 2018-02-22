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
library("grid")
library("aws.s3")

# config ----------------------------------------------------------------------
options(scipen=999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# 
rangeDate <- as.character(seq(as.Date('2018-01-01'),as.Date('2018-02-01'),by = 1))

# connect to silver ---------------------------------------------------------
drv <- dbDriver("PostgreSQL")

for(i in rangeDate){

  print (paste(i, "=> started at:", Sys.time()))

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
  FROM hydra.verticals_ninja_web_201801
  WHERE trackpage = 'listing'
  AND country_code = 'PL'
  AND host = 'www.otomoto.pl'
  AND user_id IS NOT NULL AND server_date_trunc = date_part(epoch, '##Date##')
  ;
  "

sqlQuery <- as.character(gsub("##Date##", i,  sqlQuery))

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
          object = paste0("Results_", i, ".RDS"),
          bucket = "pyrates-data-ocean/CARS/CARS-5597")
}

