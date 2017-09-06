#' CF New Onboarding for Apps 
#' Questions:
#' 1) Build segments using current parameters
#' 2) Understand cars distribution according to segment, budget and make
#' 3) Understand price filter usage: do people use the option "from" when choosing a price range?
#' Goal: show as much inventiry as we can
#' 
#' Will perform analysis using Stradia IN data. One day of Active Ads


# Load config library
library(config)
library("RMySQL")


# Establish connection to yml config

config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "stradia_in") )   

#load("~/personal_credentials.Rdata") 

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= OtomotoPlDbPwd,  # comes with loading credentials.Rdata
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname= config$DbName
)

dbListTables(conDB)


