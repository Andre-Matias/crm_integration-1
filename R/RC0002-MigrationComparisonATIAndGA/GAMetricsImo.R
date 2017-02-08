#Date of execution
ExecutedDate <- Sys.Date()

#Define the ID to ImoVirtual project in GA
ids <- "ga:45453985"

DataImoVirtual <- NULL
DataImoVirtualMonthly <- NULL

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")
#Load the file containing the Data from GoogleAnalytics
load("ExibitionImoVirtual.RData")

#Get the initial Date to execute the GA Query
if(any(DataImoVirtual$Source == "GA")) {
  dataFiltro<- as.Date(max(DataImoVirtual$Date[DataImoVirtual$Source == "GA"], 1))+1
}else{  
  dataFiltro<- as.Date("2016-01-01")
}

#Don't reprocess the same date.
if(dataFiltro != Sys.Date()){
  source("function.R")
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataImoVirtualYesterday <- QueryGA(c("ga:date"),dataFiltro)
  
  DataImoVirtualYesterday$"Entering Visits" <- DataImoVirtualYesterday$"sessions"-DataImoVirtualYesterday$"bounce"
  
  DataImoVirtualYesterday$Source[is.null(DataImoVirtualYesterday$Source)] <- "GA"
  
  # Change the Columns name
  colnames(DataImoVirtualYesterday) <- c("Date","Sessions","Page View", "Bounce","Users","Entering Visits","Source")
  
  DataImoVirtualYesterday <- DataImoVirtualYesterday[,c("Date",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataImoVirtual <- rbind(DataImoVirtual,DataImoVirtualYesterday)
  
  DataImoVirtual <- unique(DataImoVirtual, by=c("Date","SOurce"))
  
  
  DataImoVirtualMonthlyAux <- QueryGA(c("ga:Year","ga:Month"),dataFiltro)
  
  DataImoVirtualMonthlyAux$"Entering Visits" <- DataImoVirtualMonthlyAux$"sessions"-DataImoVirtualMonthlyAux$"bounce"
  
  DataImoVirtualMonthlyAux$Source[is.null(DataImoVirtualMonthlyAux$Source)] <- "GA"
  
  # Change the Columns name
  colnames(DataImoVirtualMonthlyAux) <- c("Year","Month","Sessions","Page View", "Bounce","Users","Entering Visits","Source")
  
  DataImoVirtualMonthlyAux <- DataImoVirtualMonthlyAux[,c("Year",
                                                              "Month",
                                                              "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataImoVirtualMonthly <- rbind(as.data.frame(DataImoVirtualMonthly),DataImoVirtualMonthlyAux)
  
  DataImoVirtualMonthly <-DataImoVirtualMonthly %>% group_by(Year,Month,Source) %>% summarise_each(funs(sum))
  
  #DataImoVirtualMonthly <- unique(DataImoVirtualMonthly, by=c("Year","Month","Source"))
  
  save(DataImoVirtual,DataImoVirtualMonthly,ExecutedDate, file = "ExibitionImoVirtual.RData")
}
