DataStandVirtualHourly <- NULL
DataStandVirtualHourlyShowingPhone <- NULL
DataStandVirtualHourlyShowingPhoneAux <- NULL
DataStandVirtualHourlyAux <- NULL
rawDataFromStandHourlyVirtualperDevice <- NULL
rawDataFromStandHourlyVirtualperDeviceAux <- NULL
rawDataFromStandHourlyVirtualperDeviceTemp <- NULL
rawDataFromStandHourlyVirtual <- NULL
rawDataFromStandHourlyVirtualAux <- NULL
rawDataFromStandHourlyVirtualTemp <- NULL

#Date of execution
ExecutedDateStvHourly <- Sys.Date()
ExecutedDateStvDBHourly <- Sys.Date()
ExecutedDateStvPerDeviceHourly <- Sys.Date()


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



#Get the initial Date to execute the GA Query
dataFiltro <- Sys.Date()
dataFiltroStvDB <- Sys.Date()
dataFiltroPerDevice <- Sys.Date()



  
  #####Get Desktop information on ATI#####
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?&",
                         "columns={d_device_type,d_time_hour_visit,m_visits_all,m_page_loads,m_vu,m_bounce_rate}&",
                         "sort={d_time_hour_visit}&",
                         "segment=100045213&",
                         "space={s:566290}&",
                         "period={D:'",dataFiltro,"'}&",
                         "max-results=50&",
                         "page-num=1&",
                         "apikey=46d9c0c1-6600-4137-88aa-06d7f49b30b9"
  )
  # Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
  DataStandVirtualHourlyYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyYesterday$Source[is.null(DataStandVirtualHourlyYesterday$Source)] <- "ATI"
  DataStandVirtualHourlyYesterday$Device[is.null(DataStandVirtualHourlyYesterday$Device)] <- "DESKTOP"
  DataStandVirtualHourlyYesterday$Segment[is.null(DataStandVirtualHourlyYesterday$Segment)] <- "DESKTOP"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
                                           "Segment")
  
  DataStandVirtualHourlyYesterday <- DataStandVirtualHourlyYesterday[,c("Date",
                                                            "Device",
                                                            "Segment",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Users",
                                                            "Bounce Rate")]
  
  DataStandVirtualHourlyAux <- rbind(DataStandVirtualHourlyAux,DataStandVirtualHourlyYesterday)
  
  DataStandVirtualHourlyAux <- unique(DataStandVirtualHourlyAux, by=c("Date","Source","Device"))
  
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
  DataStandVirtualHourlyYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyYesterday$Source[is.null(DataStandVirtualHourlyYesterday$Source)] <- "ATI"
  DataStandVirtualHourlyYesterday$Device[is.null(DataStandVirtualHourlyYesterday$Device)] <- "RWD"
  DataStandVirtualHourlyYesterday$Segment[is.null(DataStandVirtualHourlyYesterday$Segment)] <- "RWD"
  
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
                                           "Segment")
  
  DataStandVirtualHourlyYesterday <- DataStandVirtualHourlyYesterday[,c("Date",
                                                            "Device",
                                                            "Segment",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Users",
                                                            "Bounce Rate")]
  
  DataStandVirtualHourlyAux <- rbind(DataStandVirtualHourlyAux,DataStandVirtualHourlyYesterday)
  
  DataStandVirtualHourlyAux <- unique(DataStandVirtualHourlyAux, by=c("Date","Source","Device"))
  
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
  DataStandVirtualHourlyYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyYesterday$Source[is.null(DataStandVirtualHourlyYesterday$Source)] <- "ATI"
  DataStandVirtualHourlyYesterday$Device[is.null(DataStandVirtualHourlyYesterday$Device)] <- "ANDROID"
  DataStandVirtualHourlyYesterday$Segment[is.null(DataStandVirtualHourlyYesterday$Segment)] <- "ANDROID"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
                                           "Segment")
  
  DataStandVirtualHourlyYesterday <- DataStandVirtualHourlyYesterday[,c("Date",
                                                            "Device",
                                                            "Segment",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Users",
                                                            "Bounce Rate")]
  
  DataStandVirtualHourlyAux <- rbind(DataStandVirtualHourlyAux,DataStandVirtualHourlyYesterday)
  
  DataStandVirtualHourlyAux <- unique(DataStandVirtualHourlyAux, by=c("Date","Source","Device"))
  
  
  
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
  DataStandVirtualHourlyYesterday <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyYesterday$Source[is.null(DataStandVirtualHourlyYesterday$Source)] <- "ATI"
  DataStandVirtualHourlyYesterday$Device[is.null(DataStandVirtualHourlyYesterday$Device)] <- "IOS"
  DataStandVirtualHourlyYesterday$Segment[is.null(DataStandVirtualHourlyYesterday$Segment)] <- "IOS"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyYesterday) <- c("Date",
                                           "Sessions",
                                           "Page View",
                                           "Users", 
                                           "Bounce Rate",
                                           "Source",
                                           "Device",
                                           "Segment")
  
  DataStandVirtualHourlyYesterday <- DataStandVirtualHourlyYesterday[,c("Date",
                                                            "Device",
                                                            "Segment",
                                                            "Source",
                                                            "Sessions",
                                                            "Page View",
                                                            "Users",
                                                            "Bounce Rate")]
  
  DataStandVirtualHourlyAux <- rbind(DataStandVirtualHourlyAux,DataStandVirtualHourlyYesterday)
  
  DataStandVirtualHourlyAux <- unique(DataStandVirtualHourlyAux, by=c("Date","Source","Device"))
  
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
  DataStandVirtualHourlyShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyShowingPhoneAux$Source[is.null(DataStandVirtualHourlyShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualHourlyShowingPhoneAux$Device[is.null(DataStandVirtualHourlyShowingPhoneAux$Device)] <- "DESKTOP"
  DataStandVirtualHourlyShowingPhoneAux$Segment[is.null(DataStandVirtualHourlyShowingPhoneAux$Segment)] <- "DESKTOP"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualHourlyShowingPhoneAux <- DataStandVirtualHourlyShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualHourlyShowingPhone <- rbind(DataStandVirtualHourlyShowingPhone,DataStandVirtualHourlyShowingPhoneAux)
  
  DataStandVirtualHourlyShowingPhone <- unique(DataStandVirtualHourlyShowingPhone, by=c("Date","Source","Device"))
  
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
  DataStandVirtualHourlyShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyShowingPhoneAux$Source[is.null(DataStandVirtualHourlyShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualHourlyShowingPhoneAux$Device[is.null(DataStandVirtualHourlyShowingPhoneAux$Device)] <- "RWD"
  DataStandVirtualHourlyShowingPhoneAux$Segment[is.null(DataStandVirtualHourlyShowingPhoneAux$Segment)] <- "RWD"
  
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualHourlyShowingPhoneAux <- DataStandVirtualHourlyShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualHourlyShowingPhone <- rbind(DataStandVirtualHourlyShowingPhone,DataStandVirtualHourlyShowingPhoneAux)
  
  DataStandVirtualHourlyShowingPhone <- unique(DataStandVirtualHourlyShowingPhone, by=c("Date","Source","Device"))
  
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
  DataStandVirtualHourlyShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyShowingPhoneAux$Source[is.null(DataStandVirtualHourlyShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualHourlyShowingPhoneAux$Device[is.null(DataStandVirtualHourlyShowingPhoneAux$Device)] <- "ANDROID"
  DataStandVirtualHourlyShowingPhoneAux$Segment[is.null(DataStandVirtualHourlyShowingPhoneAux$Segment)] <- "ANDROID"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualHourlyShowingPhoneAux <- DataStandVirtualHourlyShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualHourlyShowingPhone <- rbind(DataStandVirtualHourlyShowingPhone,DataStandVirtualHourlyShowingPhoneAux)
  
  DataStandVirtualHourlyShowingPhone <- unique(DataStandVirtualHourlyShowingPhone, by=c("Date","Source","Device"))
  
  
  
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
  DataStandVirtualHourlyShowingPhoneAux <- QueryATI(urlRestAPI)
  
  DataStandVirtualHourlyShowingPhoneAux$Source[is.null(DataStandVirtualHourlyShowingPhoneAux$Source)] <- "ATI"
  DataStandVirtualHourlyShowingPhoneAux$Device[is.null(DataStandVirtualHourlyShowingPhoneAux$Device)] <- "IOS"
  DataStandVirtualHourlyShowingPhoneAux$Segment[is.null(DataStandVirtualHourlyShowingPhoneAux$Segment)] <- "IOS"
  
  # Change the Columns name
  colnames(DataStandVirtualHourlyShowingPhoneAux) <- c("Date",
                                                 "Showing Phone",
                                                 "Source",
                                                 "Device",
                                                 "Segment")
  
  DataStandVirtualHourlyShowingPhoneAux <- DataStandVirtualHourlyShowingPhoneAux[,c("Date",
                                                                        "Device",
                                                                        "Segment",
                                                                        "Source",
                                                                        "Showing Phone")]
  
  DataStandVirtualHourlyShowingPhone <- rbind(DataStandVirtualHourlyShowingPhone,DataStandVirtualHourlyShowingPhoneAux)
  
  DataStandVirtualHourlyShowingPhone <- unique(DataStandVirtualHourlyShowingPhone, by=c("Date","Source","Device"))
  
  
  DataStandVirtualHourlyAux <- merge(x=DataStandVirtualHourlyAux,y=DataStandVirtualHourlyShowingPhone, by =c("Date",
                                                                                           "Device",
                                                                                           "Segment",
                                                                                           "Source"), all.x = TRUE  )
  
  
  
  
  
  #####Add Total to DataStandVirtualHourly#####
  DT <- as.data.frame(DataStandVirtualHourlyAux %>% group_by(Date,Source) %>% summarise(Sessions = sum(Sessions), 
                                                                 'Page View' = sum(`Page View`), 
                                                                 Users = sum(Users),
                                                                 'Bounce Rate' = sum(`Bounce Rate`),
                                                                 'Showing Phone' = sum(`Showing Phone`)))
  DT$Device <- "ALL"
  DT$Segment <- "ALL"
  DataStandVirtualHourlyAux <- rbind(DataStandVirtualHourlyAux,DT)
  DataStandVirtualHourly <- rbind(DataStandVirtualHourly,DataStandVirtualHourlyAux)
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
  
  rawDataFromStandHourlyVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualperDeviceAux$Source[is.null(rawDataFromStandHourlyVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualperDeviceAux) <- c("Date",
                                                     "Action",
                                                     "Device",
                                                     "Replies - Messages",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandHourlyVirtualperDeviceAux <- rawDataFromStandHourlyVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Replies - Messages")]
  
  rawDataFromStandHourlyVirtualperDeviceTemp <- rawDataFromStandHourlyVirtualperDeviceAux
  
  
  
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
  
  rawDataFromStandHourlyVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualperDeviceAux$Source[is.null(rawDataFromStandHourlyVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualperDeviceAux) <- c("Date",
                                                     "Action",
                                                     "Device",
                                                     "Replies - Messages Cars",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandHourlyVirtualperDeviceAux <- rawDataFromStandHourlyVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Replies - Messages Cars")]
  
  rawDataFromStandHourlyVirtualperDeviceTemp <- merge(x=rawDataFromStandHourlyVirtualperDeviceTemp,y=rawDataFromStandHourlyVirtualperDeviceAux, by =c("Date",
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
  
  rawDataFromStandHourlyVirtualperDeviceAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualperDeviceAux$Source[is.null(rawDataFromStandHourlyVirtualperDeviceAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "desktop"] <- "DESKTOP"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "mob"] <- "RWD"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "and"] <- "ANDROID"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$Device == "ios"] <- "IOS"
  rawDataFromStandHourlyVirtualperDeviceAux$Segment[rawDataFromStandHourlyVirtualperDeviceAux$action == "message_olx"] <- "From OLX"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualperDeviceAux) <- c("Date",
                                                     "Action",
                                                     "Device",
                                                     "Repliers",
                                                     "Source",
                                                     "Segment")
  
  rawDataFromStandHourlyVirtualperDeviceAux <- rawDataFromStandHourlyVirtualperDeviceAux[,c("Date",
                                                                                "Device",
                                                                                "Segment",
                                                                                "Source",
                                                                                "Repliers")]
  
  rawDataFromStandHourlyVirtualperDeviceTemp <- merge(x=rawDataFromStandHourlyVirtualperDeviceTemp,y=rawDataFromStandHourlyVirtualperDeviceAux, by =c("Date",
                                                                                                                                    "Device",
                                                                                                                                    "Segment",
                                                                                                                                    "Source"), all.x = TRUE  )
  
  
  rawDataFromStandHourlyVirtualperDevice <- unique(rawDataFromStandHourlyVirtualperDevice, by=c("Date","Source","Device"))
  
  #####Add Total to DataStandVirtualHourly#####
  DTReplie <- as.data.frame(rawDataFromStandHourlyVirtualperDeviceTemp %>% 
                              group_by(Date,Source) %>% 
                              summarise('Replies - Messages' = sum(`Replies - Messages`), 
                                        'Replies - Messages Cars' = sum(`Replies - Messages Cars`), 
                                        Repliers = sum(Repliers)))
  DTReplie$Device <- "ALL"
  DTReplie$Segment <- "ALL"
  rawDataFromStandHourlyVirtualperDeviceTemp <- rbind(rawDataFromStandHourlyVirtualperDeviceTemp,DTReplie)
  rawDataFromStandHourlyVirtualperDevice <- rbind(rawDataFromStandHourlyVirtualperDevice,rawDataFromStandHourlyVirtualperDeviceTemp)
  

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
  
  rawDataFromStandHourlyVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualAux$Source[is.null(rawDataFromStandHourlyVirtualAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualAux$Device[is.null(rawDataFromStandHourlyVirtualAux$Device)] <- "ALL"
  rawDataFromStandHourlyVirtualAux$Segment[is.null(rawDataFromStandHourlyVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualAux) <- c("Date",
                                            "Action",
                                            "NNLs Privates - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandHourlyVirtualAux <- rawDataFromStandHourlyVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Privates - General")]
  
  
  rawDataFromStandHourlyVirtualTemp <- merge(x=rawDataFromStandHourlyVirtualTemp,y=rawDataFromStandHourlyVirtualAux, by =c("Date",
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
  
  rawDataFromStandHourlyVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualAux$Source[is.null(rawDataFromStandHourlyVirtualAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualAux$Device[is.null(rawDataFromStandHourlyVirtualAux$Device)] <- "ALL"
  rawDataFromStandHourlyVirtualAux$Segment[is.null(rawDataFromStandHourlyVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualAux) <- c("Date",
                                            "Action",
                                            "NNLs Dealers - General",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandHourlyVirtualAux <- rawDataFromStandHourlyVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Dealers - General")]
  
  rawDataFromStandHourlyVirtualTemp <- merge(x=rawDataFromStandHourlyVirtualTemp,y=rawDataFromStandHourlyVirtualAux, by =c("Date",
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
  
  rawDataFromStandHourlyVirtualAux <- dbGetQuery(conn_standVirtualOldDB,sqlCommand)
  
  rawDataFromStandHourlyVirtualAux$Source[is.null(rawDataFromStandHourlyVirtualAux$Source)] <- "DB"
  rawDataFromStandHourlyVirtualAux$Device[is.null(rawDataFromStandHourlyVirtualAux$Device)] <- "ALL"
  rawDataFromStandHourlyVirtualAux$Segment[is.null(rawDataFromStandHourlyVirtualAux$Segment)] <- "ALL"
  
  # Change the Columns name
  colnames(rawDataFromStandHourlyVirtualAux) <- c("Date",
                                            "Action",
                                            "NNLs Dealers - Cars",
                                            "Source",
                                            "Device",
                                            "Segment")
  
  rawDataFromStandHourlyVirtualAux <- rawDataFromStandHourlyVirtualAux[,c("Date",
                                                              "Device",
                                                              "Segment",
                                                              "Source",
                                                              "NNLs Dealers - Cars")]
  
  rawDataFromStandHourlyVirtualTemp <- merge(x=rawDataFromStandHourlyVirtualTemp,y=rawDataFromStandHourlyVirtualAux, by =c("Date",
                                                                                                         "Device",
                                                                                                         "Segment",
                                                                                                         "Source"), all.x = TRUE  )
  
  

  
  rawDataFromStandHourlyVirtual <- rbind(rawDataFromStandHourlyVirtual,rawDataFromStandHourlyVirtualTemp)
  
  rawDataFromStandHourlyVirtual <- unique(rawDataFromStandHourlyVirtual, by=c("Date","Source","Device"))
  
  #########
  
  save(DataStandVirtualHourly,
      ExecutedDateStv,
      file = "ExibitionStandVirtual.RData")
  save(rawDataFromStandHourlyVirtual,
      file = "rawDataFromStandVirtual.RData")
  save(rawDataFromStandHourlyVirtualperDevice, 
      file = "rawDataFromStandVirtualperDevice.RData")


all_cons <- dbListConnections(MySQL())
for(con in all_cons)
  dbDisconnect(con)

