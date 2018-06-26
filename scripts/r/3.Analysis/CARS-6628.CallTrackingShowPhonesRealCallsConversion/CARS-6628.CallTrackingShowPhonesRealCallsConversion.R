library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("glue")
library("RMySQL")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()


# get data from call tracking service

dfRawCallStatistics <-
  read.table("~/dump_call_statistics.txt", header = TRUE, sep = "\t")

dfRawService <-
  read.table("~/dump_service.txt", header = TRUE, sep = "\t")

dfRaw <- 
  dfRawCallStatistics %>%
  left_join(dfRawService, by = c("service_id"))

# extract user ids

strOtomotoPLUserIDs <-
  paste(
    unique(
      dfRaw[dfRaw$website_name == "otomotopl" 
            & !grepl(pattern = "test", 
                     tolower(dfRaw$external_sub_user_id)
                     ),
            c("external_user_id")]
  ),
  collapse = ","
  )

strStandvirtualUserIDs <- 
  paste(
    unique(
      dfRaw[dfRaw$website_name == "carspt" 
            & !grepl(pattern = "test", 
                     tolower(dfRaw$external_sub_user_id)
            ),
            c("external_user_id")]
    ), 
    collapse = ","
  )
    
strOtomotoPLUserIDs <- gsub("[^0-9]\\,", "", strOtomotoPLUserIDs)
strStandvirtualUserIDs <- gsub("[^0-9]\\,", "", strStandvirtualUserIDs) 

# get all Ads Ids from users

sqlQueryOtomotoPL <- 
  glue("SELECT id, user_id, status FROM ads WHERE user_id IN ({strOtomotoPLUserIDs}) AND net_ad_counted = 1")


sqlQueryStandvirtual <- 
  glue("SELECT id, user_id, status FROM ads WHERE user_id IN ({strStandvirtualUserIDs}) AND net_ad_counted = 1")

conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = "bi_team_pt",
    password = bi_team_pt_password,
    host = "127.0.0.1",
    port = 3317, 
    dbname = "otomotopl"
  )

dbSqlQuery <-
  dbSendQuery(conDB, sqlQueryOtomotoPL)

dfOtomotoPLAdsIds <- dbFetch(dbSqlQuery)

# free the result set
dbClearResult(dbSqlQuery)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)


conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = "bi_team_pt",
    password = bi_team_pt_password,
    host = "127.0.0.1",
    port = 3308, 
    dbname = "carspt"
  )

dbSqlQuery <-
  dbSendQuery(conDB, sqlQueryStandvirtual)

dfStandvirtualAdsIds <- dbFetch(dbSqlQuery)

# free the result set
dbClearResult(dbSqlQuery)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)







# connect to Yamato ----------------------------------------------------

drv <- dbDriver("PostgreSQL")

conDB <-
  dbConnect(
    drv,
    host = "10.101.5.237", # dwYamatoDbHost,
    port = dwYamatoDbPort,
    dbname = dwYamatoDbName,
    user = dwYamatoDbUsername,
    password = dwYamatoDbPassword
  )

print(paste(monthText, server_path, epochDate))
print(sqlQuery)

dbSqlQuery <-
  RPostgreSQL::dbGetQuery(
    conDB, sqlUnload
  )

dbDisconnect(conDB)
