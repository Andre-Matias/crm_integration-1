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
conn_standVirtualNewDB <- dbConnect(RMySQL::MySQL(), username = "bi", password = "bi5Zv3TB", host = "192.168.1.4", port = 3306, dbname = "carspt")

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
                       "&columns={d_time_date,m_visits,m_page_loads,m_vu,m_bounce_rate}",
                       "&sort={-m_visits}",
                       "&segment=100045213",
                       "&space={s:566290}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=ad558d4f-0520-45ea-9a06-a35021272c5e"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  DataStandVirtualYesterday$Device[is.null(DataStandVirtualYesterday$Device)] <- "DESKTOP"
  DataStandVirtualYesterday$Segment[is.null(DataStandVirtualYesterday$Segment)] <- "DESKTOP"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
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
  
  #####Get Responsive information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_visits,m_page_loads,m_vu,m_bounce_rate}",
                       "&sort={-m_visits}",
                       "&segment=100045214",
                       "&space={s:566290}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=ad558d4f-0520-45ea-9a06-a35021272c5e"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  DataStandVirtualYesterday$Device[is.null(DataStandVirtualYesterday$Device)] <- "RWD"
  DataStandVirtualYesterday$Segment[is.null(DataStandVirtualYesterday$Segment)] <- "RWD"
  
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
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
  
  #####Get Android information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_visits,m_page_loads,m_vu,m_bounce_rate}",
                       "&sort={-m_visits}",
                       "&space={s:566291}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1&apikey=a5300173-dfbb-4b37-b2c5-ec2d7672ea5f"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  DataStandVirtualYesterday$Device[is.null(DataStandVirtualYesterday$Device)] <- "ANDROID"
  DataStandVirtualYesterday$Segment[is.null(DataStandVirtualYesterday$Segment)] <- "ANDROID"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
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
  
  
  
  #####Get IOS information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_visits,m_page_loads,m_vu,m_bounce_rate}",
                       "&sort={-m_visits}",
                       "&space={s:566292}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=d70a4590-1361-4b0d-977d-cc7c6921d041"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualYesterday$Source[is.null(DataStandVirtualYesterday$Source)] <- "ATI"
  DataStandVirtualYesterday$Device[is.null(DataStandVirtualYesterday$Device)] <- "IOS"
  DataStandVirtualYesterday$Segment[is.null(DataStandVirtualYesterday$Segment)] <- "IOS"
  
  # Change the Columns name
  colnames(DataStandVirtualYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
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
  
  #####Get Desktop Showing Phone information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_clicks}",
                       "&sort={-m_clicks}",
                       "&filter={d_click:{$lk:'phone'}}",
                       "&segment=100045213&space={s:566290}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=ad558d4f-0520-45ea-9a06-a35021272c5e"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualShowingPhoneAux$Source[is.null(DataStandVirtualShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualShowingPhoneAux$Device[is.null(DataStandVirtualShowingPhoneAux$Device)] <- "DESKTOP"
  DataStandVirtualShowingPhoneAux$Segment[is.null(DataStandVirtualShowingPhoneAux$Segment)] <- "DESKTOP"
  
  # Change the Columns name
  colnames(DataStandVirtualShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualShowingPhoneAux <- DataStandVirtualShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualShowingPhone <- rbind(DataStandVirtualShowingPhone,DataStandVirtualShowingPhoneAux)
  
  DataStandVirtualShowingPhone <- unique(DataStandVirtualShowingPhone, by=c("Date","Source","Device"))
  
  #####Get Responsive Showing Phone information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_clicks}",
                       "&sort={-m_clicks}",
                       "&filter={d_click:{$lk:'phone'}}",
                       "&segment=100045214",
                       "&space={s:566290}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=ad558d4f-0520-45ea-9a06-a35021272c5e"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualShowingPhoneAux$Source[is.null(DataStandVirtualShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualShowingPhoneAux$Device[is.null(DataStandVirtualShowingPhoneAux$Device)] <- "RWD"
  DataStandVirtualShowingPhoneAux$Segment[is.null(DataStandVirtualShowingPhoneAux$Segment)] <- "RWD"
  
  
  # Change the Columns name
  colnames(DataStandVirtualShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualShowingPhoneAux <- DataStandVirtualShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualShowingPhone <- rbind(DataStandVirtualShowingPhone,DataStandVirtualShowingPhoneAux)
  
  DataStandVirtualShowingPhone <- unique(DataStandVirtualShowingPhone, by=c("Date","Source","Device"))
  
  #####Get Android Showing Phone information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_page_loads}",
                       "&sort={-m_page_loads}",
                       "&filter={cl_377983:{$lk:'phone'}}",
                       "&space={s:566291}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=a5300173-dfbb-4b37-b2c5-ec2d7672ea5f"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualShowingPhoneAux$Source[is.null(DataStandVirtualShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualShowingPhoneAux$Device[is.null(DataStandVirtualShowingPhoneAux$Device)] <- "ANDROID"
  DataStandVirtualShowingPhoneAux$Segment[is.null(DataStandVirtualShowingPhoneAux$Segment)] <- "ANDROID"
  
  # Change the Columns name
  colnames(DataStandVirtualShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualShowingPhoneAux <- DataStandVirtualShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualShowingPhone <- rbind(DataStandVirtualShowingPhone,DataStandVirtualShowingPhoneAux)
  
  DataStandVirtualShowingPhone <- unique(DataStandVirtualShowingPhone, by=c("Date","Source","Device"))
  
  
  
  #####Get IOS Showing Phone information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_page_loads}",
                       "&sort={-m_page_loads}",
                       "&filter={cl_378501:{$lk:'phone'}}",
                       "&space={s:566292}",
                       "&period={D:{start:'",dataFiltro,"',end:'",ExecutedDateStv,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=d70a4590-1361-4b0d-977d-cc7c6921d041"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualShowingPhoneAux$Source[is.null(DataStandVirtualShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualShowingPhoneAux$Device[is.null(DataStandVirtualShowingPhoneAux$Device)] <- "IOS"
  DataStandVirtualShowingPhoneAux$Segment[is.null(DataStandVirtualShowingPhoneAux$Segment)] <- "IOS"
  
  # Change the Columns name
  colnames(DataStandVirtualShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
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
  
  
  
  
  
  #####Add Total to DataStandVirtual#####
  DT <- as.data.frame(DataStandVirtualAux %>% group_by(Date,Source) %>% summarise(Sessions = sum(Sessions), 
                                                                 'Page View' = sum(`Page View`), 
                                                                 Users = sum(Users),
                                                                 'Bounce Rate' = sum(`Bounce Rate`),
                                                                 'Showing Phone' = sum(`Showing Phone`)))
  DT$Device <- "ALL"
  DT$Segment <- "ALL"
  DataStandVirtualAux <- rbind(DataStandVirtualAux,DT)
  DataStandVirtual <- rbind(DataStandVirtual,DataStandVirtualAux)
  #####Get ALL Replie Messages information on DB#####
  
  
  sqlCommand <- paste0("SELECT date(msg.date) Date,
                msg.action, 
                msg.source Device,
                count(msg.replies) 'Replies - Messages'
              FROM stv.bi_stv_messages msg 
              where msg.action IN ('message', 'message_olx')
                and date(msg.date) between '",dataFiltroPerDevice,"' and '",ExecutedDateStv,"'
                and msg.usernature = 1
              group by 1, 2,3");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Action",
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
  
  
  sqlCommand <- paste0("SELECT date(msg.date) Date, 
                          msg.action, 
                          msg.source Device, 
                          count(msg.replies) 'Replies - Messages Cars'
                          FROM stv.bi_stv_messages msg
                          inner join stv.bi_stv_dim_offer off
                          on msg.inskey = off.inskey	
                          where msg.action IN ('message', 'message_olx') 
                          and date(msg.date) between '",dataFiltroPerDevice,"' and '",ExecutedDateStv,"'
                          and msg.usernature = 1
                          and off.category = 'Carros'
                          group by 1, 2,3");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Action",
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
  
  
  sqlCommand <- paste0("SELECT date(msg.date) Date, 
                        msg.action, 
                       msg.source Device, 
                       count(distinct(msg.userid)) Repliers
                       FROM stv.bi_stv_messages msg
                       inner join stv.bi_stv_dim_offer off
                       on msg.inskey = off.inskey	
                       where msg.action IN ('message', 'message_olx') 
                       and date(msg.date) between '",dataFiltroPerDevice,"' and '",ExecutedDateStv,"'
                       and msg.usernature = 1
                       group by 1, 2,3");
  
  rawDataFromStandVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualperDeviceAux$Source[is.null(rawDataFromStandVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandVirtualperDeviceAux$Segment[rawDataFromStandVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualperDeviceAux) <- c("Date",
                                                     "Action",
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
  
  
  rawDataFromStandVirtualperDevice <- unique(rawDataFromStandVirtualperDevice, by=c("Date","Source","Device"))
  
  #####Add Total to DataStandVirtual#####
  DTReplie <- as.data.frame(rawDataFromStandVirtualperDeviceTemp %>% 
                              group_by(Date,Source) %>% 
                              summarise('Replies - Messages' = sum(`Replies - Messages`), 
                                        'Replies - Messages Cars' = sum(`Replies - Messages Cars`), 
                                        Repliers = sum(Repliers)))
  DTReplie$Device <- "ALL"
  DTReplie$Segment <- "ALL"
  rawDataFromStandVirtualperDeviceTemp <- rbind(rawDataFromStandVirtualperDeviceTemp,DTReplie)
  rawDataFromStandVirtualperDevice <- rbind(rawDataFromStandVirtualperDevice,rawDataFromStandVirtualperDeviceTemp)
  
  #####Get ALL Renewals Privates - General information on DB#####
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                        buch_aktion action, 
                        count(id) 'Renewals Privates - General' 
                        from bi_stv_movements	
                        where usernature=0 and 
                        id>0 and 
                        date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                        and buch_aktion = 'ad_renewal'
                        group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
                                            "Renewals Privates - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Renewals Privates - General")]
  
  rawDataFromStandVirtualTemp <- rawDataFromStandVirtualAux
  
  #####Get ALL Renewals Dealers - General information on DB#####
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                       buch_aktion action, 
                       count(id) 'Renewals Dealers - General' 
                       from bi_stv_movements	
                       where usernature=1 and 
                       id>0 and 
                       date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and buch_aktion = 'ad_renewal'
                       group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
                                            "Renewals Dealers - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Renewals Dealers - General")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  
  #####Get ALL Renewals Dealers - Cars information on DB#####
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                       buch_aktion action, 
                       count(id) 'Renewals Dealers - Cars' 
                       from bi_stv_movements mov
                       inner join stv.bi_stv_dim_offer off
                       on mov.buch_inskey = off.inskey		
                       where usernature=1 and 
                       id>0 and 
                       date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and buch_aktion = 'ad_renewal'
                       and off.category = 'Carros'
                       group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
                                            "Renewals Dealers - Cars",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Renewals Dealers - Cars")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  #####Get ALL NNLs Privates - General information on DB#####
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                       buch_aktion action, 
                       count(id) 'NNLs Privates - General' 
                       from bi_stv_movements	
                       where usernature=0 and 
                       id>0 and 
                       date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and buch_aktion = 'ad'
                       group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
                                            "NNLs Privates - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Privates - General")]
  
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  #####Get ALL NNLs Dealers - General information on DB#####
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                       buch_aktion action, 
                       count(id) 'NNLs Dealers - General' 
                       from bi_stv_movements	
                       where usernature=1 and 
                       id>0 and 
                       date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and buch_aktion = 'ad'
                       group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
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
  
  
  sqlCommand <- paste0("select date_format(date, '%Y-%m-%d') Date, 
                       buch_aktion action, 
                       count(id) 'NNLs Dealers - Cars' 
                       from bi_stv_movements mov
                       inner join stv.bi_stv_dim_offer off
                       on mov.buch_inskey = off.inskey		
                       where usernature=1 and 
                       id>0 and 
                       date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and buch_aktion = 'ad'
                       and off.category = 'Carros'
                       group by 1,2");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Action",
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
  
  
  #####Get ALL Rev. Listings Privates information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,	
                       (- SUM(gesamtpreis) - IFNULL(SUM(discount_buchid),0))/1.23 'Rev. Listings Privates' 
                       FROM bi_stv_movements m	
                       INNER JOIN inserent u 
                       USING(userid)	
                       LEFT JOIN 
                       (SELECT buchid, SUM(gesamtpreis)/COUNT(buchid) discount_buchid FROM bi_stv_movements WHERE buch_aktion='discount'
                         GROUP BY 1)  AS d
                       ON m.buchid=d.buchid 
                       AND buch_aktion IN ('ad','ad_renewal','api_import')  
                       WHERE buch_aktion NOT IN ('top_up','discount')	
                       AND buch_aktion IN ('ad','ad_renewal','api_import')  
                       AND date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and u.usernature = 0
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Rev. Listings Privates",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Rev. Listings Privates")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  #####Get ALL Rev. Listings Dealers information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,	
                       (- SUM(gesamtpreis) - IFNULL(SUM(discount_buchid),0))/1.2555 'Rev. Listings Dealers' 
                       FROM bi_stv_movements m	
                       INNER JOIN inserent u 
                       USING(userid)	
                       LEFT JOIN 
                       (SELECT buchid, SUM(gesamtpreis)/COUNT(buchid) discount_buchid FROM bi_stv_movements WHERE buch_aktion='discount'
                       GROUP BY 1)  AS d
                       ON m.buchid=d.buchid 
                       AND buch_aktion IN ('ad','ad_renewal','api_import')  
                       WHERE buch_aktion NOT IN ('top_up','discount')	
                       AND buch_aktion IN ('ad','ad_renewal','api_import')  
                       AND date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and u.usernature = 1
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Rev. Listings Dealers",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Rev. Listings Dealers")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  #####Get ALL Rev. VAS Privates information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,	
                       (- SUM(gesamtpreis) - IFNULL(SUM(discount_buchid),0))/1.23 'Rev. VAS Privates' 
                       FROM bi_stv_movements m	
                       INNER JOIN inserent u 
                       USING(userid)	
                       LEFT JOIN 
                       (SELECT buchid, SUM(gesamtpreis)/COUNT(buchid) discount_buchid FROM bi_stv_movements WHERE buch_aktion='discount'
                       GROUP BY 1)  AS d
                       ON m.buchid=d.buchid 
                       AND buch_aktion IN ('option_top',
                       'option_style',
                       'option_star',
                       'option_premium',
                       'option_newsletter',
                       'option_listing',
                       'option_home',
                       'option_bump',
                       'bump')    
                       WHERE buch_aktion NOT IN ('top_up','discount')
                       AND buch_aktion IN ('option_top',
                       'option_style',
                       'option_star',
                       'option_premium',
                       'option_newsletter',
                       'option_listing',
                       'option_home',
                       'option_bump',
                       'bump')    	
                       AND date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and u.usernature = 0
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Rev. VAS Privates",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Rev. VAS Privates")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  #####Get ALL Rev. VAS Dealers information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,	
                       (- SUM(gesamtpreis) - IFNULL(SUM(discount_buchid),0))/1.2555 'Rev. VAS Dealers' 
                       FROM bi_stv_movements m	
                       INNER JOIN inserent u 
                       USING(userid)	
                       LEFT JOIN 
                       (SELECT buchid, SUM(gesamtpreis)/COUNT(buchid) discount_buchid FROM bi_stv_movements WHERE buch_aktion='discount'
                       GROUP BY 1)  AS d
                       ON m.buchid=d.buchid 
                       AND buch_aktion IN ('option_top',
                       'option_style',
                       'option_star',
                       'option_premium',
                       'option_newsletter',
                       'option_listing',
                       'option_home',
                       'option_bump',
                       'bump')    
                       WHERE buch_aktion NOT IN ('top_up','discount')	
                       AND buch_aktion IN ('option_top',
                       'option_style',
                       'option_star',
                       'option_premium',
                       'option_newsletter',
                       'option_listing',
                       'option_home',
                       'option_bump',
                       'bump')   
                       AND date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       and u.usernature = 1
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Rev. VAS Dealers",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Rev. VAS Dealers")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  
  
  #####Get ALL Rev. Export to OLX information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,	
                       (- SUM(gesamtpreis) - IFNULL(SUM(discount_buchid),0))/1.2555 'Rev. Export to OLX' 
                       FROM bi_stv_movements m	
                       INNER JOIN inserent u 
                       USING(userid)	
                       LEFT JOIN 
                       (SELECT buchid, SUM(gesamtpreis)/COUNT(buchid) discount_buchid FROM bi_stv_movements WHERE buch_aktion='discount'
                       GROUP BY 1)  AS d
                       ON m.buchid=d.buchid 
                       AND buch_aktion IN ('olx_exportation')    
                       WHERE buch_aktion NOT IN ('top_up','discount')
                       AND buch_aktion IN ('olx_exportation')    	
                       AND date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"'
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Rev. Export to OLX",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Rev. Export to OLX")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  #####Get ALL Nº Packages bought information on DB#####
  
  
  sqlCommand <- paste0("SELECT DATE(date) date,
                       COUNT(id_buy) 'Number of Packages bought' 
                       FROM user_packages up
                       WHERE date_format(date, '%Y-%m-%d') between '",dataFiltroStvDB,"' and '",ExecutedDateStv,"' 
                       GROUP BY 1");
  
  rawDataFromStandVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandVirtualAux$Source[is.null(rawDataFromStandVirtualAux$Source)] <- "DB"
  rawDataFromStandVirtualAux$Device[is.null(rawDataFromStandVirtualAux$Device)] <- "ALL"
  rawDataFromStandVirtualAux$Segment[is.null(rawDataFromStandVirtualAux$Segment)] <- "ALL"
  
  
  # Change the Columns name
  colnames(rawDataFromStandVirtualAux) <- c("Date",
                                            "Number of Packages bought",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandVirtualAux <- rawDataFromStandVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "Number of Packages bought")]
  
  rawDataFromStandVirtualTemp <- merge(x=rawDataFromStandVirtualTemp,y=rawDataFromStandVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  rawDataFromStandVirtual <- rbind(rawDataFromStandVirtual,rawDataFromStandVirtualTemp)
  
  rawDataFromStandVirtual <- unique(rawDataFromStandVirtual, by=c("Date","Source","Device"))
  
  #########
  
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

