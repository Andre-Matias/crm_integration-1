library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("glue")
library("RMySQL")
library("RPostgreSQL")
library("corrplot")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

# get data from call tracking service

dfRawCallStatistics <-
  read.table("~/dump_call_statistics.txt", header = TRUE, sep = "\t")

dfRawService <-
  read.table("~/dump_service.txt", header = TRUE, sep = "\t")

# join to get service name 
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
    host = "10.29.0.140",
    port = 3320, 
    dbname = "otomotopl"
  )

dbSqlQuery <-
  dbSendQuery(conDB, sqlQueryOtomotoPL)

dfOtomotoPLAdsIds <- dbFetch(dbSqlQuery, n = -1)

# free the result set
dbClearResult(dbSqlQuery)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)


conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = "bi_team_pt",
    password = bi_team_pt_password,
    host = "10.29.0.140",
    port = 3308, 
    dbname = "carspt"
  )

dbSqlQuery <-
  dbSendQuery(conDB, sqlQueryStandvirtual)

dfStandvirtualAdsIds <- dbFetch(dbSqlQuery, n = -1)

# free the result set
dbClearResult(dbSqlQuery)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)

lstOtomotoPLAdsIds <-
  paste(dfOtomotoPLAdsIds$id, collapse = ",")

lstStandvirtualAdsIds <-
  paste(dfStandvirtualAdsIds$id, collapse = ",")

sqlQueryOtomotoPL <-
  "
  SELECT server_path, ad_id, server_date_day, COUNT(*)QtyShowPhoneReplies
  FROM main.hydra_verticals.web
  WHERE trackname = 'reply_phone_show'
  AND server_path IN ('/h/v-otomoto-web', '/h/v-otomoto-ios', '/h/v-otomoto-android')
  AND ad_id IN({lstOtomotoPLAdsIds})
  GROUP BY 1, 2, 3
  ;
  "

sqlQueryOtomotoPL <- glue(sqlQueryOtomotoPL)

sqlQueryStandvirtualPT <-
  "
  SELECT server_path, ad_id, server_date_day, COUNT(*)QtyShowPhoneReplies
  FROM main.hydra_verticals.web
  WHERE trackname = 'reply_phone_show'
  AND server_path IN ('/h/v-standvirtual-web', '/h/v-standvirtual-ios', '/h/v-standvirtual-android')
  AND ad_id IN({lstStandvirtualAdsIds})
  GROUP BY 1, 2, 3
  ;
  "

sqlQueryStandvirtualPT <- glue(sqlQueryStandvirtualPT)

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

# OtomotoPL

#web
resSqlQueryOtomotoPL_web <-
  dbSendQuery(
    conDB, sqlQueryOtomotoPL
  )

dfSqlQueryOtomotoPL_web <- fetch(resSqlQueryOtomotoPL_web, n = -1)

# ios
sqlQueryOtomotoPL_ios <- gsub("main.hydra_verticals.web", "main.hydra_verticals.ios", sqlQueryOtomotoPL)

resSqlQueryOtomotoPL_ios <-
  dbSendQuery(
    conDB, sqlQueryOtomotoPL_ios
  )

dfSqlQueryOtomotoPL_ios <- fetch(resSqlQueryOtomotoPL_ios, n = -1)

# android
sqlQueryOtomotoPL_and <- gsub("main.hydra_verticals.web", "main.hydra_verticals.android", sqlQueryOtomotoPL)

resSqlQueryOtomotoPL_and <-
  dbSendQuery(
    conDB, sqlQueryOtomotoPL_and
  )

dfSqlQueryOtomotoPL_and <- fetch(resSqlQueryOtomotoPL_and, n = -1)

dfSqlQueryOtomotoPL <- rbind(dfSqlQueryOtomotoPL_web, dfSqlQueryOtomotoPL_ios, dfSqlQueryOtomotoPL_and)


# StandvirtualPT

#web
resSqlQueryStandvirtualPT_web <-
  dbSendQuery(
    conDB, sqlQueryStandvirtualPT
  )

dfSqlQueryStandvirtualPT_web <- fetch(resSqlQueryStandvirtualPT_web, n = -1)

# ios
sqlQueryStandvirtualPT_ios <- gsub("main.hydra_verticals.web", "main.hydra_verticals.ios", sqlQueryStandvirtualPT)

resSqlQueryStandvirtualPT_ios <-
  dbSendQuery(
    conDB, sqlQueryStandvirtualPT_ios
  )

dfSqlQueryStandvirtualPT_ios <- fetch(resSqlQueryStandvirtualPT_ios, n = -1)

# android
sqlQueryStandvirtualPT_and <- gsub("main.hydra_verticals.web", "main.hydra_verticals.android", sqlQueryStandvirtualPT)

resSqlQueryStandvirtualPT_and <-
  dbSendQuery(
    conDB, sqlQueryStandvirtualPT_and
  )

dfSqlQueryStandvirtualPT_and <- fetch(resSqlQueryStandvirtualPT_and, n = -1)

dfSqlQueryStandvirtualPT <- rbind(dfSqlQueryStandvirtualPT_web, dfSqlQueryStandvirtualPT_ios, dfSqlQueryStandvirtualPT_and)

dbDisconnect(conDB)


# OTOMOTO
dfShowPhoneByUserByDayOtomotoPL <-
  dfSqlQueryOtomotoPL %>%
  group_by(ad_id) %>%
  summarise(qtyshowphonereplies=sum(qtyshowphonereplies)) %>%
  inner_join(dfOtomotoPLAdsIds, by = c("ad_id" = "id")) %>%
  group_by(user_id) %>%
  summarise(qtyshowphonereplies=sum(qtyshowphonereplies)) %>%
  select(user_id, qtyshowphonereplies)
  
dfCallsByUserByDayOtomotoPL <-
  dfRaw %>%
  mutate(call_start_date = as.Date(call_start)) %>%
  filter(website_name == 'otomotopl', call_start_date >= as.Date('2018-05-19')) %>%
  group_by(external_user_id) %>%
  summarise(qtyCalls = sum(n())) %>%
  right_join(dfShowPhoneByUserByDayOtomotoPL, by = c("external_user_id" = "user_id")) %>%
  mutate(avgShowPhonesPerCall = qtyshowphonereplies / qtyCalls)

dfCallsByUserByDayOtomotoPL[is.na(dfCallsByUserByDayOtomotoPL)] <- 0

ggplot(dfCallsByUserByDayOtomotoPL)+
  geom_point(aes(qtyCalls, qtyshowphonereplies))+
  geom_smooth(aes(qtyCalls, qtyshowphonereplies))+
  ggtitle("OTOMOTO - Qty Show Phone Vs Qty Tracked Calls")+
  theme_gdocs()

res <- cor(dfCallsByUserByDayOtomotoPL[, c("qtyCalls", "qtyshowphonereplies")])

corrplot(res, type = "upper",
         tl.col = "black", tl.srt = 0, addCoef.col = TRUE)

ggplot(dfCallsByUserByDayOtomotoPL)+
  geom_density(aes(x=avgShowPhonesPerCall), binwidth = 1)+
  geom_vline(aes(xintercept=median(avgShowPhonesPerCall)),
                color="blue", linetype="dashed", size=1)+
  scale_x_continuous(limits = c(0,15), breaks = seq(0,15,1))+
  scale_y_continuous(labels = scales::percent, limits = c(0,0.16), breaks = seq(0,0.16,0.025))+
  ggtitle("OTOMOTO - Avg Show Phone Actions by Tracked Call")+
  theme_gdocs()