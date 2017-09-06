# Load config library
library(config)

# Establish connection to yml config

config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "otomoto_pl") )   # to extract from Otomoto Poland for example

config <- config::get(file = "~/verticals-bi/yml_config/config.yml",
                      config = Sys.getenv("R_CONFIG_ACTIVE", "stradia_ar") )   # Autovito Romania

# config <- config::get()   # to get default


# Extract config paramenters
config$DbPort
config$DbHost
config$DbName


# Load personal credentials (passwords)
load("~/verticals-bi/yml_config/credentials.Rdata") # personal credentials


# Example incorporating yml credentials in a DB connection to Otomoto Pl

library("RMySQL")

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= OtomotoPlDbPwd,  # comes with loading credentials.Rdata
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname = config$DbName
)

dbListTables(conDB)

dbDisconnect(conDB)




#############
conDB<- dbConnect(MySQL(), 
                  user= "bi_team_pt", 
                  password= "bi5Zv3TB",  # comes with loading credentials.Rdata
                  host= "192.168.1.5", 
                  port= 3320,
                  dbname = "otomotopl"
)
