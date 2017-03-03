
DataHeavyMachinery <- NULL
HeavyMachineryYesterday <- NULL
                            
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

source("function.R")

#Get the initial Date to execute the GA Query
if(any(DataHeavyMachinery$Source == "GA")) {
  dataFiltro <- as.Date(max(DataHeavyMachinery$Date[DataHeavyMachinery$Source == "GA"], 1))+1
}else{  
  dataFiltro<- as.Date("2017-02-28")
  
}

dataFiltro<- as.Date("2017-02-27")
ExecutedDateHM <- as.Date("2017-02-27")

DataHeavyMachinery <- NULL
HeavyMachineryYesterday <- NULL
DT <- NULL

#Don't reprocess the same date.
if(dataFiltro != Sys.Date()){
  
  #####Get Desktop information on ATI#####
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  HeavyMachineryYesterday <- QueryGA(c("ga:date","ga:deviceCategory"),dataFiltro)
  
  
  HeavyMachineryYesterday$"Entering Visits" <- HeavyMachineryYesterday$"sessions"-HeavyMachineryYesterday$"bounces"
  HeavyMachineryYesterday$Source[is.null(HeavyMachineryYesterday$Source)] <- "GA"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "desktop"] <- "DESKTOP"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "mobile"] <- "RWD"
  HeavyMachineryYesterday$Segment[HeavyMachineryYesterday$deviceCategory == "tablet"] <- "RWD"
  
  colnames(HeavyMachineryYesterday) <- c("Date",
                                         "Device",
                                         "Sessions",
                                         "Page View", 
                                         "Bounce Rate",
                                         "Users",
                                         "Bounces",
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
                                                        "Bounce Rate",
                                                        "Entering Visits",
                                                        "Users")]
  
  HeavyMachineryYesterday <- unique(HeavyMachineryYesterday, by=c("Date","Segment","Source"))
  
  #####Add Total to DataStandVirtual#####
  DT <- as.data.frame(HeavyMachineryYesterday %>% group_by(Date,Source) %>% summarise(Sessions = sum(Sessions), 
                                                                                  'Page View' = sum(`Page View`), 
                                                                                  Users = sum(Users),
                                                                                  Bounces = sum(Bounces),
                                                                                  'Bounce Rate' = sum(`Bounce Rate`),
                                                                                  'Entering Visits' = sum(`Entering Visits`)))
  DT$Device <- "ALL"
  DT$Segment <- "ALL"
  
  HeavyMachineryYesterday <- rbind(HeavyMachineryYesterday,DT)
  
  DataHeavyMachinery <- rbind(DataHeavyMachinery,HeavyMachineryYesterday)
  
  DataHeavyMachinery <- unique(DataHeavyMachinery, by=c("Date","Segment","Source"))
 
  #########
  
  save(DataHeavyMachinery,
    ExecutedDateHM,
     file = "ExibitionHeavyMachinery.RData")
}

