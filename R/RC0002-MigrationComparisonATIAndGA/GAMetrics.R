#Date of execution
ExecutedDate <- Sys.Date()

#Define the ID to StandVirtual project in GA
ids <- "ga:536318"

DataStandVirtual <- NULL
DataStandVirtualMonthly <- NULL

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")
#Load the file containing the Data from GoogleAnalytics
load("ExibitionStandVirtual.RData")

#Get the initial Date to execute the GA Query
if(any(DataStandVirtual$Source == "GA")) {
  dataFiltro<- as.Date(max(DataStandVirtual$Date[DataStandVirtual$Source == "GA"], 1))+1
}else{  
  dataFiltro<- as.Date("2016-01-01")
}

#Don't reprocess the same date.
if(dataFiltro != Sys.Date()){
  source("function.R")
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryGA(c("ga:date"),dataFiltro)
  
  DataStandVirtualYesterday$"Entering Visits" <- DataStandVirtualYesterday$"sessions"-DataStandVirtualYesterday$"bounce"
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "GA"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date","Sessions","Page View", "Bounce","Users","Entering Visits","Source")
  
  DataStandVirtualYesterday <- DataStandVirtualYesterday[,c("Date",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataStandVirtual <- rbind(DataStandVirtual,DataStandVirtualYesterday)
  
  DataStandVirtual <- unique(DataStandVirtual, by=c("Date","SOurce"))
  
  
  DataStandVirtualMonthlyAux <- QueryGA(c("ga:Year","ga:Month"),dataFiltro)
  
  DataStandVirtualMonthlyAux$"Entering Visits" <- DataStandVirtualMonthlyAux$"sessions"-DataStandVirtualMonthlyAux$"bounce"
  
  DataStandVirtualMonthlyAux$Source[is.null(DataStandVirtualMonthlyAux$Source)] <- "GA"
  
  # Change the Columns name
  colnames(DataStandVirtualMonthlyAux) <- c("Year","Month","Sessions","Page View", "Bounce","Users","Entering Visits","Source")
  
  DataStandVirtualMonthlyAux <- DataStandVirtualMonthlyAux[,c("Year",
                                                              "Month",
                                                              "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataStandVirtualMonthly <- rbind(as.data.frame(DataStandVirtualMonthly),DataStandVirtualMonthlyAux)
  
  
  DataStandVirtualMonthly <-DataStandVirtualMonthly %>% group_by(Year,Month,Source) %>% summarise_each(funs(sum))
  
  
  save(DataStandVirtual,DataStandVirtualMonthly,ExecutedDate, file = "ExibitionStandVirtual.RData")
}