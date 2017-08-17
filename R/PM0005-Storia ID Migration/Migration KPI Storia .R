
#libraries 

library(RGA)
library(dplyr)
library("RMySQL")
library(googleVis)
library(stringr)
library(readxl)
library(ggplot2)
library("scales")
library("lubridate")

#----------------load--------------------------------------------------------------------------------

# 
# ClientId <- "877237948354-itkbp7b4s6ausl9rpu5kfl5u2ee2mv5h.apps.googleusercontent.com"
# ClientSecret <- "66AiUOVHVZt-ROA0nsIz7ktF"
# Clientusername <- "pedro.matos@olx.com"
# 
# 
# # Authorize the Google Analytics account
# ga_tokenGA <- authorize(username = Clientusername,
#           client.id = ClientId,
#           client.secret = ClientSecret,
#            cache = TRUE, reauth = FALSE, token = NULL)
# save(ga_tokenGA, file = "tokenGA.RData")

#Define the ID to Imo GA
load("UsersStorTotal.RData")
ids <- "ga:114231327"
idsios <- "ga:113515530"
idsand <- "ga:113522807"
#startdate <- "2017-05-01"
startdate <- max(UsersStorTotal$Date)


#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

# Push the data from GA - Visits and Users 


AdpageVisitsy <- data.frame(get_ga(profileId = ids, start.date = startdate,
                                        end.date = "yesterday", metrics = c("ga:sessions"), 
                                        dimensions = c("ga:date"),  sort = NULL, filters = "ga:pagePath=@/ad",
                                        segment = NULL, samplingLevel = NULL, start.index = NULL,
                                        max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))

UsersStory <- data.frame(get_ga(profileId = ids, start.date = startdate,
                                end.date = "yesterday", metrics = c("ga:users"), 
                                dimensions = c("ga:date","ga:deviceCategory"),  sort = NULL, filters = NULL,
                                segment = NULL, samplingLevel = NULL, start.index = NULL,
                                max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))


UsersStorSEOy <- data.frame(get_ga(profileId = ids, start.date = startdate,
                                end.date = "yesterday", metrics = c("ga:users"), 
                                dimensions = c("ga:date","ga:sourceMedium"),  sort = NULL, filters = NULL,
                                segment = 'gaid::-5', samplingLevel = NULL, start.index = NULL,
                                max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))

UsersStorDirecty <- data.frame(get_ga(profileId = ids, start.date = startdate,
                                   end.date = "yesterday", metrics = c("ga:users"), 
                                   dimensions = c("ga:date","ga:sourceMedium"),  sort = NULL, filters = NULL,
                                   segment = 'gaid::-7', samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))


UsersStorPaidy <- data.frame(get_ga(profileId = ids, start.date = startdate,
                                      end.date = "yesterday", metrics = c("ga:users"), 
                                      dimensions = c("ga:date","ga:sourceMedium"),  sort = NULL, filters = NULL,
                                      segment = 'gaid::-4', samplingLevel = NULL, start.index = NULL,
                                      max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))




#Load Users Data from GA - Yesterday - Apps 


UsersStoriosy <- data.frame(get_ga(profileId = idsios, start.date = startdate,
                                   end.date = "yesterday", metrics = c("ga:users"), 
                                   dimensions = c("ga:date"),  sort = NULL, filters = NULL,
                                   segment = NULL, samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))



UsersStorandy <- data.frame(get_ga(profileId = idsand, start.date = startdate,
                                   end.date = "yesterday", metrics = c("ga:users"), 
                                   dimensions = c("ga:date"),  sort = NULL, filters = NULL,
                                   segment = NULL, samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))



#Connect DB 

#old DB 

cmd_storia <- 'ssh -i ~/marco_pasin_key biuser@52.74.90.117 -p 10022 -L 10001:172.50.21.59:3306 -N'

system(cmd_storia, wait=FALSE)

conn_storia <-  dbConnect(RMySQL::MySQL(), username = "biuser", password = "SPwE57nX", host = "127.0.0.1", port = 10001, dbname = "realestate_id")

#new DB 

#conn_storianew <-  dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3315)




#load message replies old 
StoriaReplies <- "SELECT  DATE_FORMAT(posted,'%y-%m-%d') as Date, COUNT(*) FROM realestate_id.answers WHERE spam_status IN ('ok','probably_ok') AND user_id = seller_id AND buyer_id = sender_id AND parent_id = 0 AND posted >= '2017-05-01' GROUP BY 1"
StoriaReplies <- dbGetQuery(conn_storia,StoriaReplies)

#load message replies new 
#StoriaRepliesNew <- "SELECT  DATE_FORMAT(posted,'%y-%m-%d') as Date, COUNT(*) FROM storiaid.answers WHERE spam_status IN ('ok','probably_ok') AND user_id = seller_id AND buyer_id = sender_id AND parent_id = 0 AND posted >= '2017-08-18' GROUP BY 1"
#StoriaRepliesNew <- dbGetQuery(conn_storianew,StoriaRepliesNew)

#Activeads - If you have some problem with the script run the query directly in DB....and then save it on excel file to keep the data
#Activeads <- read_excel("Activeadsid.xlsx")



#load Active Ads old
Activeadsd <- "SELECT COUNT(*) as 'Active ads' FROM realestate_id.ads WHERE STATUS = 'active'"
Activeadsd <- dbGetQuery(conn_storia,Activeadsd)

#load Active Ads new 
#Activeadsd <- "SELECT COUNT(*) as 'Active ads' FROM storiaid.ads WHERE STATUS = 'active'"
#Activeadsd <- dbGetQuery(conn_storianew,Activeadsd)


#load Active listers 
#Activelistersd <- "SELECT COUNT(DISTINCT users.id) as 'Active listers' FROM realestate_id.users INNER JOIN realestate_id.ads ON users.id=ads.user_id WHERE type = 'confirmed' AND status = 'active'"
#Activelistersd <- dbGetQuery(conn_storia,Activelistersd)

#load NNL old
NNL <- "SELECT DATE_FORMAT(created_at_first,'%y-%m-%d') as Date,COUNT(*) as NNL FROM realestate_id.ads WHERE created_at_first >= '2017-05-01' AND net_ad_counted = 1 GROUP BY 1;"
NNL<- dbGetQuery(conn_storia,NNL)


#load NNL new
#NNLnew <- "SELECT DATE_FORMAT(created_at_first,'%y-%m-%d') as Date,COUNT(*) as NNL FROM storiaid.ads WHERE created_at_first >= '2017-08-18' AND net_ad_counted = 1 GROUP BY 1;"
#NNLnew <- dbGetQuery(conn_storianew,NNLnew)


dbDisconnect(conn_storia)

#dbDisconnect(conn_storianew)



#---------------------------------transform-------------------------------------------------------------------------------------------------------------


#Tranform DAU - Yesterday
UsersStory$date <- as.POSIXct(UsersStory$date, tz = "WIB")
UsersStorandy$date <- as.POSIXct(UsersStorandy$date, tz = "WIB")
UsersStoriosy$date <- as.POSIXct(UsersStoriosy$date, tz = "WIB")
#UsersStoriosy$date <- as.Date(UsersStoriosy$date, tz = "WIB")
UsersStory <- aggregate(cbind(users) ~ date, data = UsersStory, sum)
colnames(UsersStory) <- c("Date","web")
UsersStory$Date <- as.POSIXct(UsersStory$Date, tz = "WIB")
Userappsy <- merge(UsersStoriosy,UsersStorandy,by=c("date"))
colnames(Userappsy) <- c("Date","Ios","Android")
UsersStorTotaly <- cbind(UsersStory,Userappsy)
UsersStorTotaly <- UsersStorTotaly[c(1,2,4,5)]
UsersStorTotaly <-  UsersStorTotaly %>% group_by(Date) %>% summarise(Users = sum(web,Ios,Android))
colnames(UsersStorTotaly) <- c("Date","DAU")
#save(UsersStorTotal,file="UsersStorTotal.RData")

# merge DAU - Total 

UsersStorTotal$Date <- as.Date(DAU$Date, tz = "CET")
UsersStorTotaly$Date <- as.Date(UsersStorTotaly$Date, tz = "CET")

UsersStorTotal <- UsersStorTotal[!(UsersStorTotal$Date %in% UsersStorTotaly$Date),]
UsersStorTotal <- rbind(UsersStorTotal,UsersStorTotaly)

save(UsersStorTotal,file="UsersStorTotal.RData")


# DAU by Sources - Yesterday

UserSEO <- aggregate(cbind(users) ~ date, data = UsersStorSEOy,sum)
UsersStorPaidy <- UsersStorPaidy[c(1,3)]
UsersStorDirecty <- UsersStorDirecty[c(1,3)]
UserSEO$date <- as.POSIXct(UserSEO$date, tz = "WIB")
UsersStorDirecty$date <- as.POSIXct(UsersStorDirecty$date, tz = "WIB")
UsersStorPaidy$date <- as.POSIXct(UsersStorPaidy$date, tz = "WIB")
UsersSourcey <- cbind(UserSEO,UsersStorDirecty,UsersStorPaidy)
UsersSourcey <- UsersSourcey[c(1,2,4,6)]
colnames(UsersSourcey) <- c("Date","Organic Users","Direct Users","Paid Users")


# merge DAU by source - Total 
load("UsersSource.RData")
UsersSource$Date <- as.Date(UsersSource$Date, tz = "CET")
UsersSourcey$Date <- as.Date(UsersSourcey$Date, tz = "CET")
UsersSource <- UsersSource[!(UsersSource$Date %in% UsersSourcey$Date),]
UsersSource <- rbind(UsersSource,UsersSourcey)
save(UsersSource,file="UsersSource.RData")



#Replies 

StoriaReplies$Date <- as.Date(StoriaReplies$Date, format = "%y-%m-%d")
colnames(StoriaReplies) <- c("Date","Replies")

#Replies to do after migration 
#StoriaRepliesNew$Date <- as.Date(StoriaRepliesNew$Date, format = "%y-%m-%d")
#StoriaReplies <- StoriaReplies[!(StoriaReplies %in% StoriaRepliesNew)]
#StoriaReplies <- rbind(StoriaReplies,StoriaRepliesNew)

save(StoriaReplies,file="StoriaReplies.RData")


# Transform Ad Visits 
load('AdpageVisits.RData')
#AdpageVisits <- rbind(AdpageVisitsy,AdpageVisits)
colnames(AdpageVisitsy) <- c("Date","Ad Page Visits")
AdpageVisitsy$Date <- as.Date(AdpageVisitsy$Date, tz = "CET")
AdpageVisits <- AdpageVisits[!(AdpageVisits$Date %in% AdpageVisitsy$Date),]
AdpageVisits <- rbind(AdpageVisits,AdpageVisitsy)
save(AdpageVisits,file="AdpageVisits.RData")


#CR by Ad Visits 

ConversionRate <- merge(AdpageVisits,StoriaReplies, by =c("Date"))
ConversionRate$CR <- percent(ConversionRate$Replies/ConversionRate$`Ad Page Visits`)
ConversionRate <- ConversionRate[c(1,4)]
ConversionRate$CR <- as.numeric(sub("%","",ConversionRate$CR))
save(ConversionRate,file="ConversionRate.RData")
#load("ConversionRate.RData")




# Transform Active ads - Today 
vecactive <- list(Sys.Date(),Activeadsd)
save(vecactive,file="vecactive.RData")
#load("vecactive.Rdata")

xdate <- vecactive[1]
Active <- vecactive[2]

dfactiveads <- data.frame(unlist(xdate),unlist(Active))
dfactiveads$unlist.xdate. <- as.Date(dfactiveads$unlist.xdate.,origin="1970-01-01")
colnames(dfactiveads) <- c("Date","Active Ads")


#merge Activeads - Total 
load("Activeads.RData")
#Activeads <- Activeads[order(as.Date(Activeads$Date, format="%d/%m/%Y")),]
Activeads$Date <- as.Date(Activeads$Date, format = "%y-%m-%d")
Activeads <- Activeads[!(Activeads$Date %in% dfactiveads$Date),]
Activeads <- rbind(Activeads,dfactiveads)
Activeads$`Active Ads` <- as.numeric(Activeads$`Active Ads` )
rownames(Activeads) <- NULL
save(Activeads,file="Activeads.RData")


# Transform NNL 
NNL$Date <- as.Date(NNL$Date, format = "%y-%m-%d")

#nnl to do after migration 
#NNLnew$Date <- as.Date(NNLnew$Date,format = "%y-%m-%d")
#NNL <- NNL[!(NNL$Date %in% NNLnew$Date)]
#NNL <- rbind(NNL,NNLnew)

save(NNL,file="nnlstoria.RData")
#load("nnlstoria.RData")






