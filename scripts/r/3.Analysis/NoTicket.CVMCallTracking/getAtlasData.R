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
    #c("OtomotoPL", 3317, "otomotopl", "29"),
    #c("AutovitRO", 3315, "autovitro", "29"),
    #c("StandvirtualPT", 3308, "carspt", "29"),
    c("OtodomPL", 3318, "otodompl", "101, 102, 103, 201, 202, 203"),
    c("ImovirtualPT",3309,"imovirtualpt", "101, 102, 103, 201, 202, 203"),
    c("StoriaRO", 3314 ,"storiaro", "101, 102, 103, 201, 202, 203")
  )

for(vertical in dbs){
  print(vertical[1])
  print(vertical[2])
  print(vertical[3])
  
  catID = vertical[4]
  
  # define query
  
  querySQl <-
    "
  SELECT A.id,
  A.phone,
  A.user_id,
  A.created_at_first,
  A.params,
  A.category_id
  FROM ads A
  INNER JOIN users U
  ON A.user_id=U.id
  WHERE
  U.is_business = 1
  AND A.created_at_first >= '2018-02-01 00:00:00'
  AND A.created_at_first < '2018-03-01 00:00:00'
  AND A.net_ad_counted = 1
  AND category_id IN({catID})
  ;
  "
  
  querySQl <- glue(querySQl)
  
  querySQl3 <- "SELECT * FROM users_business;"
  
  # extract also stands for standvirtual --------------------------------------
  
  print(vertical[1] == "StandvirtualPT")
  
  if(vertical[1] == "StandvirtualPT"){
    querySQl <- gsub("A.user_id,", "A.user_id, A.stand_id,", querySQl)
  }
  
  if(vertical[1] %in% c("OtodomPL", "ImovirtualPT", "StoriaRO")){
    querySQl <- gsub("A.user_id,", "A.user_id, A.agent_id,", querySQl)
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
  saveRDS(object = dfSqlQuery, file = paste0("~/CT/", "dfAds_", vertical[1], ".RDS"))
  
  dfSqlQuery3 <-
    dbGetQuery(conDB, querySQl3)
  
  assign(paste0("dfUsers_", vertical[1]), value = dfSqlQuery3)
  saveRDS(object = dfSqlQuery3, file = paste0("~/CT/", "dfUsers_", vertical[1], ".RDS"))
  
  
  if(vertical[1] == "StandvirtualPT"){
    
    querySQl2 <- 
      "SELECT * FROM stand;"
    
    print(querySQl2)
    
    rs <- dbSendQuery(conDB, 'set character set "utf8"')
    
    dfSqlQuery2 <-
      dbGetQuery(conDB, querySQl2)
    
    assign(paste0("dfStands_", vertical[1]), value = dfSqlQuery2)
    saveRDS(object = dfSqlQuery2, file = paste0("~/CT/", "dfStands_", vertical[1], ".RDS"))
    
  }
  
  if(vertical[1] %in% c("OtodomPL", "ImovirtualPT", "StoriaRO")){
    
    querySQl2 <- 
      "SELECT * FROM agents;"
    
    print(querySQl2)
    
    rs <- dbSendQuery(conDB, 'set character set "utf8"')
    
    dfSqlQuery2 <-
      dbGetQuery(conDB, querySQl2)
    
    assign(paste0("dfAgents_", vertical[1]), value = dfSqlQuery2)
    saveRDS(object = dfSqlQuery2, file = paste0("~/CT/", "dfAgents_", vertical[1], ".RDS"))
    
  }
  
  dbDisconnect(conDB)
}

