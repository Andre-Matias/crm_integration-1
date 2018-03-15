# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("showtext")
library("RPostgreSQL")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
font_add_google("Open Sans", "opensans")

dbs <-
  list(
    c("OtomotoPL", 3317, "otomotopl"),
    c("AutovitRO", 3315, "autovitro"),
    c("StandvirtualPT", 3308, "carspt")
  )

for(vertical in dbs){
  print(vertical[1])
  print(vertical[2])
  print(vertical[3])
  
  # define query
  
  querySQl <-
    "
  SELECT A.id,
  A.phone,
  A.user_id,
  A.created_at_first,
  A.params
  FROM ads A
  INNER JOIN users U
  ON A.user_id=U.id
  WHERE
  U.is_business = 1
  AND A.created_at_first >= '2018-02-01 00:00:00'
  AND A.created_at_first < '2018-03-01 00:00:00'
  AND A.net_ad_counted = 1
  AND category_id = 29
  ;
  "
  querySQl3 <- "SELECT * FROM users_business;"
  
  # extract also stands for standvirtual --------------------------------------
  
  print(vertical[1] == "StandvirtualPT")
  
  if(vertical[1] == "StandvirtualPT"){
    querySQl <- gsub("A.user_id,", "A.user_id, A.stand_id,", querySQl)
  }
  
  # connect to database  ------------------------------------------------------
  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(vertical[2]), 
      dbname = vertical[3]
    )
  
  rs <- dbSendQuery(conDB, 'set character set "utf8"')
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  assign(paste0("dfAds_", vertical[1]), value = dfSqlQuery)
  
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl3)
  
  assign(paste0("dfUsers_", vertical[1]), value = dfSqlQuery)
  
  if(vertical[1] == "StandvirtualPT"){
    
    querySQl2 <- 
      "SELECT * FROM stand;"
    
    print(querySQl2)
    
    rs <- dbSendQuery(conDB, 'set character set "utf8"')
    
    dfSqlQuery2 <-
      dbGetQuery(conDB, querySQl2)
    
    assign(paste0("dfStands_", vertical[1]), value = dfSqlQuery2)
    
  }
  
  dbDisconnect(conDB)
}