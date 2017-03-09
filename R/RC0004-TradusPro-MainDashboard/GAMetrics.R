
DataHeavyMachinery <- NULL
HeavyMachineryYesterday <- NULL
DataHeavyMachineryDB <- NULL
HeavyMachineryDBYesterday <- NULL
                            
#Load the file containing the Data from GoogleAnalytics
load("ExibitionHeavyMachinery.RData")

#Date of execution
ExecutedDateHM <- Sys.Date()

#Define the ID to StandVirtual project in GA
ids <- "ga:140259587"

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
cmd_traduspro <- 'plink.exe -i RodrigodeCaroPrivateKey.ppk -N -batch  -ssh -L 10000:172.60.20.136:3306 biuser@54.229.200.159 -P 10022'

system(cmd_traduspro, wait=FALSE)
Sys.sleep(5)

conn_traduspro <-  dbConnect(RMySQL::MySQL(), username = "biuser", password = "v5a2XoAJ", host = "127.0.0.1", port = 10000)


#Get the initial Date to execute the GA Query
if(any(DataHeavyMachinery$Source == "GA")) {
  dataFiltroGA <- as.Date(max(DataHeavyMachinery$Date[DataHeavyMachinery$Source == "GA"], 1))+1
}else{  
  dataFiltroGA<- as.Date("2017-03-01")
}

#Get the initial Date to execute the DB Query
if(any(DataHeavyMachineryDB$Source == "DB")) {
  dataFiltroDB <- as.Date(max(DataHeavyMachineryDB$Date[DataHeavyMachineryDB$Source == "DB"], 1))+1
}else{  
  dataFiltroDB<- as.Date("2017-03-01")
}

#Don't reprocess the same date to GA Metrics
if(dataFiltroGA != Sys.Date()){
  
  #####Get Desktop information on ATI#####
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  HeavyMachineryYesterday <- QueryGA(c("ga:date","ga:deviceCategory"),dataFiltroGA)
  
  
  HeavyMachineryYesterday$date <- as.Date.POSIXct(HeavyMachineryYesterday$date,tz = "CET")
  
  HeavyMachineryYesterday$"Entering Visits" <- HeavyMachineryYesterday$"sessions"-HeavyMachineryYesterday$"bounces"
  HeavyMachineryYesterday$Source[is.null(HeavyMachineryYesterday$Source)] <- "GA"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "desktop"] <- "DESKTOP"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "mobile"] <- "RWD"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "tablet"] <- "RWD"
  
  colnames(HeavyMachineryYesterday) <- c("Date",
                                         "Device",
                                         "Sessions",
                                         "Page View", 
                                         "Users",
                                         "Bounces",
                                         "Entrance",
                                         "Entering Visits",
                                         "Source",
                                         "Segment")
  
  
  HeavyMachineryYesterday <- HeavyMachineryYesterday[,c("Date",
                                                        "Device",
                                                        "Segment",
                                                        "Source",
                                                        "Sessions",
                                                        "Page View",
                                                        "Bounces",
                                                        "Entering Visits",
                                                        "Users",
                                                        "Entrance")]
  
  HeavyMachineryYesterday <- unique(HeavyMachineryYesterday, by=c("Date","Segment","Source"))
  
  #####Add Total to DataStandVirtual#####
  DT <- as.data.frame(HeavyMachineryYesterday %>% group_by(Date,Source) %>% summarise(Sessions = sum(Sessions), 
                                                                                  'Page View' = sum(`Page View`), 
                                                                                  Users = sum(Users),
                                                                                  Bounces = sum(Bounces),
                                                                                  'Entering Visits' = sum(`Entering Visits`),
                                                                                  Entrance = sum(Entrance)))
  DT$Device <- "ALL"
  DT$Segment <- "ALL"
  
  HeavyMachineryYesterday <- rbind(HeavyMachineryYesterday,DT)
  
  
  HeavyMachineryYesterday$"Bounce Rate" <- HeavyMachineryYesterday$"Bounces"/HeavyMachineryYesterday$"Entrance"
  
  DataHeavyMachinery <- rbind(DataHeavyMachinery,HeavyMachineryYesterday)
  
  DataHeavyMachinery <- unique(DataHeavyMachinery, by=c("Date","Segment","Source"))
 
  
  #########
  
  save(DataHeavyMachinery,
    ExecutedDateHM,
     file = "ExibitionHeavyMachinery.RData")
}



