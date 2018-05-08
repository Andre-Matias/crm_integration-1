# load libraries --------------------------------------------------------------
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("showtext")
library("glue")
library("aws.s3")
library("lubridate")

load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")


listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

dfAll <- as_tibble()

for(i in listAccounts){
  
  querySQl <-
    "
    SELECT id, params 
    FROM ads 
    WHERE status = 'active' 
    AND category_id = 29
    AND net_ad_counted = 1
    ;
    "

  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(i[[3]]), 
      dbname = i[[4]]
    )
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  dbDisconnect(conDB)
  
  dfSqlQuery$project <- as.character(i[[1]])
  
  if(nrow(dfAll) == 0){
    dfAll <- dfSqlQuery
  } else {
    dfAll <- rbind(dfAll, dfSqlQuery)
  }
}