#Date of execution
ExecutedDateStv <- Sys.Date()

DataStandVirtual <- NULL
DataStandVirtualMonthly <- NULL

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")
#Load the file containing the Data from GoogleAnalytics
load("ExibitionStandVirtual.RData")

#Get the initial Date to execute the GA Query
if(any(DataStandVirtual$Source == "ATI")) {
  dataFiltro<- as.Date(max(DataStandVirtual$Date[DataStandVirtual$Source == "ATI"], 1))+1
}else{  
  dataFiltro<- as.Date("2016-01-01")
}

#Don't reprocess the same date.
if(dataFiltro != Sys.Date()){
  source("function.R")
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(dataFiltro,ExecutedDateStv-1)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date","Sessions","Page View", "Bounce","Entering Visits","Users","Source")
  
  DataStandVirtualYesterday <- DataStandVirtualYesterday[,c("Date",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataStandVirtual <- rbind(DataStandVirtual,DataStandVirtualYesterday)
  
  DataStandVirtual <- unique(DataStandVirtual, by=c("Date","SOurce"))
  
  
  DataStandVirtualMonthlyAux <- QueryATIMonth(dataFiltro,ExecutedDateStv-1)
  
  DataStandVirtualMonthlyAux$Source[is.null(DataStandVirtualMonthlyAux$Source)] <- "ATI"
  
  # Change the Columns name
  colnames(DataStandVirtualMonthlyAux) <- c("Month","Year","Sessions","Page View", "Bounce","Entering Visits","Users","Source")
  
  DataStandVirtualMonthlyAux$Month <- sprintf("%02d",DataStandVirtualMonthlyAux$Month) # fix to 2 characters
  
  DataStandVirtualMonthlyAux <- DataStandVirtualMonthlyAux[,c("Year",
                                                              "Month",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataStandVirtualMonthly <- rbind(as.data.frame(DataStandVirtualMonthly),DataStandVirtualMonthlyAux)
  
  DataStandVirtualMonthly <- unique(DataStandVirtualMonthly, by=c("Date","SOurce"))
  
  save(DataStandVirtual,DataStandVirtualMonthly,ExecutedDate,ExecutedDateStv, file = "ExibitionStandVirtual.RData")
}

