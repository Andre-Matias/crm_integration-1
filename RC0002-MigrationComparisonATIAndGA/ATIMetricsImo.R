#Date of execution
ExecutedDateImo <- Sys.Date()

DataImoVirtual <- NULL
DataImoVirtualMonthly <- NULL

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")
#Load the file containing the Data from GoogleAnalytics
load("ExibitionImoVirtual.RData")

#Get the initial Date to execute the GA Query
if(any(DataImoVirtual$Source == "ATI")) {
  dataFiltro<- as.Date(max(DataImoVirtual$Date[DataImoVirtual$Source == "ATI"], 1))+1
}else{  
  dataFiltro<- as.Date("2016-01-01")
}

#Don't reprocess the same date.
if(dataFiltro != Sys.Date()){
  source("function.R")
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataImoVirtualYesterday <- QueryATIImo(dataFiltro,ExecutedDateImo-1)
  
  DataImoVirtualYesterday$Source[is.null(DataImoVirtualYesterday$Source)] <- "ATI"
  
  # Change the Columns name
  colnames(DataImoVirtualYesterday) <- c("Date","Sessions","Page View", "Bounce","Entering Visits","Users","Source")
  
  DataImoVirtualYesterday <- DataImoVirtualYesterday[,c("Date",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Bounce",
                                                            "Entering Visits",
                                                            "Users")]
  
  DataImoVirtual <- rbind(DataImoVirtual,DataImoVirtualYesterday)
  
  DataImoVirtual <- unique(DataImoVirtual, by=c("Date","Source"))
  
  
  DataImoVirtualMonthlyAux <- QueryATIImoMonth(dataFiltro,ExecutedDateImo-1)
  
  DataImoVirtualMonthlyAux$Source[is.null(DataImoVirtualMonthlyAux$Source)] <- "ATI"
  
  # Change the Columns name
  colnames(DataImoVirtualMonthlyAux) <- c("Month","Year","Sessions","Page View", "Bounce","Entering Visits","Users","Source")
  
  DataImoVirtualMonthlyAux$Month <- sprintf("%02d",DataImoVirtualMonthlyAux$Month) # fix to 2 characters
  
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
  
  save(DataImoVirtual,DataImoVirtualMonthly,ExecutedDate,ExecutedDateImo, file = "ExibitionImoVirtual.RData")
}

