# libraries -------------------------------------------------------------------
library("RMySQL")
library("feather")
library("stringr")


# config ---------------------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

# read arguments -------------------------------------------------------------

##First read in the arguments listed at the command line
args <- (commandArgs(TRUE))

##args is now a list of character vectors
## First check to see if arguments are passed.
## Then cycle through each element of the list and evaluate the expressions.
if(length(args)==0){
  print("No arguments supplied.")
  ##supply default values
  vertical <-"OtomotoPL"
}else{
  for(i in 1:length(args)){
    eval(parse(text=args[[i]]))
    #print(args[[i]])
  }
}

print(vertical)

# select vertical -------------------------------------------------------------
if(!exists("vertical")){
  vertical <- "xvz"
}

dbUser <- get(paste0("cf", vertical, "DbUser")) 
dbPass <- bi_team_pt_password
dbHost <- as.character(
  ifelse(Sys.info()["nodename"] == "bisb", "127.0.0.1"
         , get(paste0("cf", vertical, "DbHost")))
)
dbPort <- as.numeric(get(paste0("cf", vertical, "DbPort")))
dbName <- get(paste0("cf", vertical, "DbName")) 


# -----------------------------------------------------------------------------
dates <- as.character(seq(as.Date("2017-06-15"), as.Date(Sys.Date()-1), "days"))

files <- list.files(path = '/data/lake/',
                    pattern = paste0('^RDL.*', vertical,'.*','ads_history' ,'.*feather$'),
                    full.names = TRUE
)

if(length(files) > 0){
  existingdates <- str_match(files, 
                             paste0("(RDL)",
                                    "_(", 
                                    vertical, 
                                    ")_(",
                                    "[a-z\\_].*",
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
      "/data/lake/RDL_",
      vertical, 
      "_ads_history_", 
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
      "SELECT `primary`, changed_at, id, `status`, params",
      "FROM ads_history.ads_history_otomotopl", 
      "WHERE changed_at", 
      "BETWEEN '", i, " 00:00:00'", 
      "AND '", i, " 23:59:59';"
    )
  system.time({
  dbSqlQuery <-
    dbGetQuery(conDB,dbSqlQuery)
  }
  )
  
  # disconnect from database  -------------------------------------------------
  dbDisconnect(conDB)
  
  # write file to disk --------------------------------------------------------
  write_feather(x = dbSqlQuery, path = filename)
}
