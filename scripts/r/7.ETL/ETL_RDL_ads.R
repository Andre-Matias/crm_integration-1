# libraries -------------------------------------------------------------------
library("RMySQL")
library("feather")
library("stringr")


# config ---------------------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

# select vertical -------------------------------------------------------------
if(!exists("vertical")){
  vertical <- "OtomotoPL"
}

dbUser <- get(paste0("cf", vertical, "DbUser")) 
dbPass <- biUserPassword
dbHost <- as.character(
  ifelse(Sys.info()["nodename"] == "bisb", "127.0.0.1"
         , get(paste0("cf", vertical, "DbHost")))
)
dbPort <- as.numeric(get(paste0("cf", vertical, "DbPort")))
dbName <- get(paste0("cf", vertical, "DbName")) 


# -----------------------------------------------------------------------------
dates <- as.character(seq(as.Date("2017-03-01"), as.Date(Sys.Date()-1), "days"))

files <- list.files(path = '/home/daniel.rocha/datalake/',
                    pattern = paste0('^RDL.*', vertical,'.*','ads' ,'.*feather$'),
                    full.names = TRUE
)

if(length(files) > 0){
  existingdates <- str_match(files, 
                             paste0("(RDL)",
                                    "_(", 
                                    vertical, 
                                    ")_(",
                                    "[a-z].*",
                                    ")_(",
                                    "[0-9\\-]{10})"
                             )
  )
  dates <- dates[!(dates %in% existingdates)] 
}

# start loop for every missing date -----------------------------------------

for (i in dates){
  
  print(i)
  filename <- 
    paste0(
      "/home/daniel.rocha/datalake/RDL_",
      vertical, 
      "_ads_", 
      i, 
      ".feather"
    )
  
  print(filename)
  
  # connect to database  ------------------------------------------------------
  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = dbUser,
      password = dbPass,
      host = dbHost,
      port = dbPort, 
      dbname = dbName
    )
  
  # get data ------------------------------------------------------------------
  dbSqlQuery <-
    paste(
      "SELECT * FROM ads", 
      "WHERE created_at_first", 
      "BETWEEN '", i, " 00:00:00'", 
      "AND '", i, " 23:59:59';"
    )
  
  dbSqlQuery <-
    dbGetQuery(conDB,dbSqlQuery)
  
  
  # disconnect from database  -------------------------------------------------
  dbDisconnect(conDB)
  
  # write file to disk --------------------------------------------------------
  write_feather(x = dbSqlQuery, path = filename)
}
