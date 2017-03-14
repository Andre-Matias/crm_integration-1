
DataTradus <- NULL
TradusYesterday <- NULL
DataTradusDB <- NULL
TradusDBYesterday <- NULL
                            
#Load the file containing the Data from GoogleAnalytics
load("ExibitionTradus.RData")

#Date of execution
ExecutedDateHM <- Sys.Date()

#Define the ID to StandVirtual project in GA
ids <- "ga:141010400"

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

#Load Rcurl package
library("bitops")
#Load Rcurl package
library("RCurl")
#load packge
library("jsonlite")
#load MySQL Package
library("RMySQL")
#load Data Table
library("dplyr")
# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)

source("Function.R")

## get data from Tradus Pro DB
# cmd_traduspro <- 'plink.exe -i RodrigodeCaroPrivateKey.ppk -N -batch  -ssh -L 10000:172.60.20.136:3306 biuser@54.229.200.159 -P 10022'
# 
# system(cmd_traduspro, wait=FALSE)
# Sys.sleep(5)
# 
# conn_traduspro <-  dbConnect(RMySQL::MySQL(), username = "biuser", password = "v5a2XoAJ", host = "127.0.0.1", port = 10000)


#Get the initial Date to execute the GA Query
if(any(DataTradus$Source == "GA")) {
  dataFiltroGA <- as.Date(max(DataTradus$Date[DataTradus$Source == "GA"], 1))+1
}else{  
  dataFiltroGA<- as.Date("2017-03-01")
}

#Get the initial Date to execute the DB Query
if(any(DataTradusDB$Source == "DB")) {
  dataFiltroDB <- as.Date(max(DataTradusDB$Date[DataTradusDB$Source == "DB"], 1))+1
}else{  
  dataFiltroDB<- as.Date("2017-03-01")
}

#Don't reprocess the same date to GA Metrics
if(dataFiltroGA != Sys.Date()){
  
  #####Get Desktop information on ATI#####
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  TradusYesterday <- QueryGA(c("ga:date","ga:deviceCategory"),dataFiltroGA)
  
  
  TradusYesterday$date <- as.Date.POSIXct(TradusYesterday$date,tz = "CET")
  
  TradusYesterday$"Entering Visits" <- TradusYesterday$"sessions"-TradusYesterday$"bounces"
  TradusYesterday$Source[is.null(TradusYesterday$Source)] <- "GA"
  TradusYesterday$Segment[TradusYesterday$deviceCategory == "desktop"] <- "DESKTOP"
  TradusYesterday$Segment[TradusYesterday$deviceCategory == "mobile"] <- "RWD"
  TradusYesterday$Segment[TradusYesterday$deviceCategory == "tablet"] <- "RWD"
  
  colnames(TradusYesterday) <- c("Date",
                                         "Device",
                                         "Sessions",
                                         "Page View", 
                                         "Users",
                                         "Bounces",
                                         "Entrance",
                                         "Entering Visits",
                                         "Source",
                                         "Segment")
  
  
  TradusYesterday <- TradusYesterday[,c("Date",
                                                        "Device",
                                                        "Segment",
                                                        "Source",
                                                        "Sessions",
                                                        "Page View",
                                                        "Bounces",
                                                        "Entering Visits",
                                                        "Users",
                                                        "Entrance")]
  
  TradusYesterday <- unique(TradusYesterday, by=c("Date","Segment","Source"))
  
  #####Add Total to DataStandVirtual#####
  DT <- as.data.frame(TradusYesterday %>% group_by(Date,Source) %>% summarise(Sessions = sum(Sessions), 
                                                                                  'Page View' = sum(`Page View`), 
                                                                                  Users = sum(Users),
                                                                                  Bounces = sum(Bounces),
                                                                                  'Entering Visits' = sum(`Entering Visits`),
                                                                                  Entrance = sum(Entrance)))
  DT$Device <- "ALL"
  DT$Segment <- "ALL"
  
  TradusYesterday <- rbind(TradusYesterday,DT)
  
  
  TradusYesterday$"Bounce Rate" <- TradusYesterday$"Bounces"/TradusYesterday$"Entrance"
  
  DataTradus <- rbind(DataTradus,TradusYesterday)
  
  DataTradus <- unique(DataTradus, by=c("Date","Segment","Source"))
 
  
  #########
  
  save(DataTradus,
    ExecutedDateHM,
     file = "ExibitionTradus.RData")
}



