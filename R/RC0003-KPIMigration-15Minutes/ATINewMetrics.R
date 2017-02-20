DataStandVirtual <- NULL
DataStandVirtualShowingPhone <- NULL
DataStandVirtualShowingPhoneAux <- NULL
DataStandVirtualAux <- NULL
rawDataFromStandVirtualperDevice <- NULL
rawDataFromStandVirtualperDeviceAux <- NULL
rawDataFromStandVirtualperDeviceTemp <- NULL
rawDataFromStandVirtual <- NULL
rawDataFromStandVirtualAux <- NULL
rawDataFromStandVirtualTemp <- NULL

#Load the file containing the Data from GoogleAnalytics
load("ExibitionStandVirtual.RData")
#Load the file containing the Data from GoogleAnalytics
load("rawDataFromStandVirtual.RData")
#Load the file containing the Data from GoogleAnalytics
load("rawDataFromStandVirtualperDevice.RData")
#Date of execution
ExecutedDateStv <- Sys.Date()
ExecutedDateStvDB <- Sys.Date()
ExecutedDateStvPerDevice <- Sys.Date()


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

source("function.R")

conn_standVirtualOldDB <- dbConnect(RMySQL::MySQL(), username = "bi_readonly", password = "bi3vDBAM", host = "192.168.1.5", port = 3307, dbname = "stv")
conn_standVirtualNewDB <- dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3308, dbname = "carspt")

DataStandVirtual <- DataStandVirtual[!DataStandVirtual$Date == Sys.Date(),]
rawDataFromStandVirtual <- rawDataFromStandVirtual[!rawDataFromStandVirtual$Date == Sys.Date(),]
rawDataFromStandVirtualperDevice <- rawDataFromStandVirtualperDevice[!rawDataFromStandVirtualperDevice$Date == Sys.Date(),]


#Get the initial Date to execute the GA Query
dataFiltro <- Sys.Date()
dataFiltroStvDB <- Sys.Date()
dataFiltroPerDevice <- Sys.Date()



  
  #####Get Desktop information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,cd_platfv2,m_visits,m_page_loads,m_vu,m_bounce_rate}",
                       "&sort={-m_visits}",
                       "&space={s:579542}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=91455269-1a05-4cf3-91dc-830b33674c56"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  DataStandVirtualYesterday$Segment[DataStandVirtualYesterday$cd_platfv2 == "ios"] <- "IOS"
  DataStandVirtualYesterday$Segment[DataStandVirtualYesterday$cd_platfv2 == "i2"] <- "RWD"
  DataStandVirtualYesterday$Segment[DataStandVirtualYesterday$cd_platfv2 == "desktop"] <- "DESKTOP"
  DataStandVirtualYesterday$Segment[DataStandVirtualYesterday$cd_platfv2 == "android"] <- "ANDROID"
  DataStandVirtualYesterday$Segment[DataStandVirtualYesterday$cd_platfv2 == "N/A"] <- "OTHERS"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date",
                                           "Device",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Segment")
  
  DataStandVirtualYesterday <- DataStandVirtualYesterday[,c("Date",
                                                            "Device",
                                                            "Segment",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Users",
                                                            "Bounce Rate")]
  
  DataStandVirtualAux <- rbind(DataStandVirtualAux,DataStandVirtualYesterday)
  
  DataStandVirtualAux <- unique(DataStandVirtualAux, by=c("Date","Source","Device"))
  
  #####Get Showing Phone information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,cd_platfv2,m_clicks}",
                       "&sort={-m_clicks}",
                       "&filter={d_click:{$lk:'phone'}}",
                       "&space={s:579542}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=91455269-1a05-4cf3-91dc-830b33674c56"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualShowingPhoneAux$Source[is.null(DataStandVirtualShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualShowingPhoneAux$Segment[DataStandVirtualShowingPhoneAux$cd_platfv2 == "ios"] <- "IOS"
  DataStandVirtualShowingPhoneAux$Segment[DataStandVirtualShowingPhoneAux$cd_platfv2 == "i2"] <- "RWD"
  DataStandVirtualShowingPhoneAux$Segment[DataStandVirtualShowingPhoneAux$cd_platfv2 == "desktop"] <- "DESKTOP"
  DataStandVirtualShowingPhoneAux$Segment[DataStandVirtualShowingPhoneAux$cd_platfv2 == "android"] <- "ANDROID"
  DataStandVirtualShowingPhoneAux$Segment[DataStandVirtualShowingPhoneAux$cd_platfv2 == "N/A"] <- "OTHERS"
  
  # Change the Columns name
  colnames(DataStandVirtualShowingPhoneAux) <- c("Date",
                                                 "Device",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Segment")
  
  DataStandVirtualShowingPhoneAux <- DataStandVirtualShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualShowingPhone <- rbind(DataStandVirtualShowingPhone,DataStandVirtualShowingPhoneAux)
  
  DataStandVirtualShowingPhone <- unique(DataStandVirtualShowingPhone, by=c("Date","Source","Device"))
  
  
  DataStandVirtualAux <- merge(x=DataStandVirtualAux,y=DataStandVirtualShowingPhone, by =c("Date",
                                                                                           "Device",
                                                                                           "Segment",
                                                                                           "Source"), all.x = TRUE  )
  
  
  DataStandVirtual <- rbind(DataStandVirtual,DataStandVirtualAux)
  
  #####Get ALL Replie Messages information on DB#####
  
  
  sqlCommand <- paste0("SELECT date_format(posted, '%Y-%m-%d') date, 
                       source Device, count(*) Replies FROM carspt.answers
                       where date_format(posted, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"'
                       and parent_id = 0
                       and user_id = sender_id
                       group by 1,2");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "rwd"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "android"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "apple"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "none"] <- "OTHER"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Device",
                                                     "Replies - Messages",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandVirtualperDeviceAux <- rawDataFromStandVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Replies - Messages")]
  
  rawDataFromStandVirtualperDeviceTemp <- rawDataFromStandVirtualperDeviceAux
  
  
  
  #####Get ALL Replie Messages Cars information on DB#####
  
  
  sqlCommand <- paste0("SELECT date_format(posted, '%Y-%m-%d') date, 
                       ans.source Device, 
                       count(*) Replies 
                       FROM carspt.answers ans
                       Inner JOin  carspt.ads ads
                       ON ans.ad_id = ads.id
                       where date_format(posted, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"'
                       and parent_id = 0
                       and ans.user_id = sender_id
                       and ads.category_id = 29
                       group by 1,2");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "rwd"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "android"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "apple"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "none"] <- "OTHER"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Device",
                                                     "Replies - Messages Cars",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandVirtualperDeviceAux <- rawDataFromStandVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Replies - Messages Cars")]
  
  rawDataFromStandVirtualperDeviceTemp <- merge(x=rawDataFromStandVirtualperDeviceTemp,y=rawDataFromStandVirtualperDeviceAux, by =c("Date",
                                                                                                                                    "Device",
                                                                                                                                    "Segment",
                                                                                                                                    "Source"), all.x = TRUE  )
  
  #####Get ALL Repliers information on DB#####
  
  
  sqlCommand <- paste0("SELECT date_format(posted, '%Y-%m-%d') date, 
                       source Device, 
                       count(distinct user_id) Repliers 
                       FROM carspt.answers
                       where date_format(posted, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"' 
                       and parent_id = 0
                       and user_id = sender_id
                       group by 1,2");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "rwd"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "android"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "apple"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "none"] <- "OTHER"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Device",
                                                     "Repliers",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandVirtualperDeviceAux <- rawDataFromStandVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Repliers")]
  
  rawDataFromStandVirtualperDeviceTemp <- merge(x=rawDataFromStandVirtualperDeviceTemp,y=rawDataFromStandVirtualperDeviceAux, by =c("Date",
                                                                                                                                    "Device",
                                                                                                                                    "Segment",
                                                                                                                                    "Source"), all.x = TRUE  )
  
  rawDataFromStandVirtualperDevice <- rbind(rawDataFromStandVirtualperDevice,rawDataFromStandVirtualperDeviceTemp)
  
  rawDataFromStandVirtualperDevice <- unique(rawDataFromStandVirtualperDevice, by=c("Date","Source","Device"))
  
  
  
  
  
  #####Get ALL NNLs Privates - General information on DB#####
  
  sqlCommand <- paste0("SELECT date_format(created_at_first, '%Y-%m-%d') Date, 
                        count(ad.id) 'NNLs Privates - General' 
                        FROM carspt.ads ad
                        Inner JOIN carspt.users user
                        ON ad.user_id = user.id
                        where  date_format(created_at_first, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"'
                        and user.is_business = 0
                        group by 1" );
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "NNLs Privates - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Privates - General")]
  
  
  rawDataFromStandVirtualTemp <- rawDataFromStandVirtualAux
  
  #####Get ALL NNLs Dealers - General information on DB#####
  
  
  sqlCommand <- paste0("SELECT date_format(created_at_first, '%Y-%m-%d') Date, 
                        count(ad.id) 'NNLs Privates - General' 
                        FROM carspt.ads ad
                        Inner JOIN carspt.users user
                        ON ad.user_id = user.id
                        where  date_format(created_at_first, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"'
                        and user.is_business = 1
                        group by 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "NNLs Dealers - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Dealers - General")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  
  #####Get ALL NNLs Dealers - Cars information on DB#####
  
  
  sqlCommand <- paste0("SELECT date_format(created_at_first, '%Y-%m-%d') Date, 
                        count(ad.id) 'NNLs Privates - General' 
                       FROM carspt.ads ad
                       Inner JOIN carspt.users user
                       ON ad.user_id = user.id
                       where  date_format(created_at_first, '%Y-%m-%d') between '",dataFiltro,"' and '",ExecutedDateStv,"'
                       and user.is_business = 1
                       and ad.category_id = 29
                       group by 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualNewDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "NNLs Dealers - Cars",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Dealers - Cars")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  rawDataFromStandVirtualTemp$`Number of Packages bought`<- 0
  rawDataFromStandVirtualTemp$`Renewals Privates - General`<- 0
  rawDataFromStandVirtualTemp$`Renewals Dealers - General`<- 0
  rawDataFromStandVirtualTemp$`Renewals Dealers - Cars`<- 0
  rawDataFromStandVirtualTemp$`Rev. Listings Privates`<- 0
  rawDataFromStandVirtualTemp$`Rev. Listings Dealers`<- 0
  rawDataFromStandVirtualTemp$`Rev. VAS Privates`<- 0
  rawDataFromStandVirtualTemp$`Rev. VAS Dealers`<- 0
  rawDataFromStandVirtualTemp$`Rev. Export to OLX`<- 0

  rawDataFromStandVirtual <- rbind(rawDataFromStandVirtual,rawDataFromStandVirtualTemp)
  
  rawDataFromStandVirtual <- unique(rawDataFromStandVirtual, by=c("Date","Source","Device"))
  
  save(DataStandVirtual,
       ExecutedDateStv,
       file = "ExibitionStandVirtual.RData")
  save(rawDataFromStandVirtual,
       file = "rawDataFromStandVirtual.RData")
  save(rawDataFromStandVirtualperDevice, 
       file = "rawDataFromStandVirtualperDevice.RData")


all_cons <- dbListConnections(MySQL())
for(con in all_cons)
  dbDisconnect(con)
