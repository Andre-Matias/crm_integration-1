# libraries -------------------------------------------------------------------
library("config")
library("RMySQL")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("stringr")
library("ggthemes")
library("scales")
library("lubridate")

#for(site in c("standvirtual_pt", "otomoto_pl", "autovit_ro")){

for(site in c("otomoto_pl")){
  
  # load db configurations ------------------------------------------------------
  config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                        config = Sys.getenv("R_CONFIG_ACTIVE", site)
  )
  
  # -----------------------------------------------------------------------------
  load("~/credentials.Rdata")
  
  # get data --------------------------------------------------------------------
  conDB<- 
    dbConnect(MySQL(), 
              user= config$DbUser, 
              password= bi_team_pt_password,  
              host = "127.0.0.1",
              port = as.numeric(config$BiServerPort),
              dbname = config$DbName
    )
  
  dbGetQuery(conDB, "SET NAMES utf8")

  cmdSqlQuery <-
  "
    SELECT * FROM otomotopl.answers 
    WHERE posted >= '2017-09-01 00:00:00'
    AND posted < '2018-01-01 00:00:00'
  "
  dbSendQuery(conDB, 'set character set "utf8"')
  
  dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)
  dfMessages <- as_tibble(dfQueryResults)
  dbDisconnect(conDB)

  rm("dfQueryResults")

}

# save data to local storage TODO: save it to S3 ------------------------------
saveRDS(dfMessages, "~/dfMessages.RDS")

# remove spam and sort messages -----------------------------------------------
dfMessages <- 
  dfMessages %>%
  filter(spam_status != "spam") %>%
  arrange(buyer_id, seller_id, ad_id, posted)

# keep only the first message sent by the buyer -------------------------------
dfMessages_L0 <-
  dfMessages %>%
  filter(parent_id == 0,
         seller_id != buyer_id,
         sender_id == buyer_id,
         user_id == buyer_id)

# dfMessages %>%
#   filter(ad_id==6017051865
#          & seller_id == 5459197
#          & buyer_id == 68772
#          ) %>%
#   View()
# 
# dfMessages %>%
# filter(ad_id==6016087575
#        & seller_id == 1571411
#        & buyer_id == 69408
#        ) %>%
#   View()



dfStats <- 
  dfMessages_L0 %>%
  filter(posted >= '2017-09-01-01 00:00:00' & posted < '2017-12-01-01 00:00:00') %>%
  group_by(topics_count) %>%
  summarise(qtyTopicsCount = sum(n())) %>%
  mutate(perTopicsCount = qtyTopicsCount / sum(qtyTopicsCount))

dfMessages_T1 <- 
  dfMessages %>%
  filter(spam_status != "spam") %>%
  filter(topics_count == 1) %>%
  arrange(buyer_id, seller_id, ad_id, posted) 