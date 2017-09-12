# libraries -------------------------------------------------------------------
library("config")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("stringr")

# load db configurations ------------------------------------------------------
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "standvirtual_pt") )

# -----------------------------------------------------------------------------
load("~/credentials.Rdata")

# get data

library("RMySQL")

conDB<- 
  dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= bi_team_pt_password,  
                  host = "127.0.0.1", 
                  port = as.numeric(config$BiServerPort),
                  dbname = config$DbName
            )



cmdSqlQuery <- 
  "
    SELECT *
    FROM carspt.ads
    WHERE
      category_id = 661
      AND status = 'active'
    ;
  "

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)

dbDisconnect(conDB)

dfAds <- dfQueryResults

rm("dfQueryResults")
