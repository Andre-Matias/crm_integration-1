# libraries -------------------------------------------------------------------
library("RMySQL")
library("feather")
library("stringr")


# config ---------------------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

# read arguments -------------------------------------------------------------

vertical <-"OtomotoPL"

# select vertical -------------------------------------------------------------
if(!exists("vertical")){
  vertical <- "xvz"
}

dbUser <- cfOtomotPLDbUser 
dbPass <- biUserPassword
dbHost <- as.character(
  ifelse(Sys.info()["nodename"] == "bisb", "127.0.0.1"
         , get(paste0("cf", vertical, "DbHost")))
)
dbPort <- as.numeric(get(paste0("cf", vertical, "DbPort")))
dbName <- get(paste0("cf", vertical, "DbName")) 

  
# connect to database  ------------------------------------------------------
conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = cfOtomotoPLDbUser,
    password = bi_team_pt_password,
    host = "127.0.0.1",
    port = 3317, 
    dbname = cfOtomotoPLDbName
  )
  
  # get data ------------------------------------------------------------------
  dbSqlQuery <-
  "
  SELECT id_ad, name, name_en, code, type, duration, price,
  date, last_status_date, status, PS.provider
  FROM paidads_user_payments PUP
  INNER JOIN paidads_indexes PI
  ON PUP.id_index=PI.id
  INNER JOIN payment_session PS
  ON PUP.id_transaction=PS.id
  WHERE type IN ('topads', 'highlight', 'ad_homepage', 'export_olx',
                 'bump_up', 'paid_for_post', 'ad_bighomepage')
  AND PS.status = 'finished'
  AND date BETWEEN '2016-11-01 00:00:00' AND '2017-07-01 00:00:00';
  "
  
  dfSqlQuery <-
    dbGetQuery(conDB, dbSqlQuery)
  
  
# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)
  

# save active ads df ----------------------------------------------------------
s3saveRDS(x = dfSqlQuery,
          object = "CARS-5165/OtomotoVAS.RDS",
          bucket = s3BucketName)