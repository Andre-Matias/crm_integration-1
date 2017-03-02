#library 
#load package to connect to Mysql
library("RMySQL")
library("DBI")
library("dplyr")
library("dtplyr")
library("data.table")
library("stringdist")
library("ggplot2")
library("reshape2")
library("plyr")
library(plotly)
library(formattable)
library("scales")
library(googleVis)
library(stringr)
library(xlsx)




#connect Storia DB 
#Storia Vertical In Database
#Database host: 172.30.100.29:3306
#Datase User: biuser
#Datase Pass: SPwE57nX

#connect Stradia DB 
#Stradia Vertical In Database
#Database host: 172.50.21.59:3306
#Datase User: biuser
#Datase Pass: SPwE57nX

#In database with SSh tunnel you will also need SSH Key
#ssh key: bi_id_rsa.ppk
#ssh user: biuser
"ssh host: 52.74.90.117:10022

#In database with SSh tunnel you will also need SSH Key
#ssh key: bi_id_rsa.ppk
#ssh user: biuser
#ssh host: 134.213.201.219:10022

#In  Windows you can create the tunnel with plink.exe

#string variable to create plink command

cmd_storia <- 'C:/Jump/plink.exe -i C:/Jump/bi.ppk -N -batch  -ssh -L 10002:172.50.21.59:3306 biuser@52.74.90.117 -P 10022'

cmd_stradia <- 'C:/Jump/plink.exe -i C:/Jump/id_rsa.ppk -N -batch  -ssh -L 10002:172.30.100.29:3306 inbi_r@134.213.201.219 -P 10022'

#wait 5 sec
Sys.sleep(5)

#run plink command

shell(cmd_storia, wait=FALSE)

shell(cmd_stradia, wait=FALSE)

#wait 5 sec



# connect to Database
conn_storia <-  dbConnect(RMySQL::MySQL(), username = "biuser", password = "SPwE57nX", host = "127.0.0.1", port = 10002, dbname = "realestate_id")

conn_stradia <-  dbConnect(RMySQL::MySQL(), username = "inbi_r", password = "Vz0b61BY", host = "127.0.0.1", port = 10002, dbname = "cars_in")


# Create SQL Statement
Storiadup <- "SELECT id, user_id, city_id, category_id, title,REPLACE(SUBSTRING_INDEX(params, '<br>price[currency]', 1), 'price<=>price<br>price<=>', '') as price FROM realestate_id.ads WHERE status = 'active';"

Stradiadup <- "SELECT id, user_id, description,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>version<=><br>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage FROM cars_in.ads WHERE status = 'active';"


RawStoriaDup <- dbGetQuery(conn_storia,Storiadup)

RawStradiaDup <- dbGetQuery(conn_stradia,Stradiadup)

dbDisconnect(conn_storia)
dbDisconnect(conn_stradia)


##Find duplications 

------------storia--------------------------------------------------------------------------------------------

#load Raw Data (Total Ads by day + Duplication by day)

load("RawStoriaDup.RData")
#load("RawStoriafinal.RData")
#save(RawStoriaDup,file="RawStoriaDup.RData")
#save(RawStoriafinal,file="RawStoriafinal.RData")


#Do the crossjoin for ad id and keep other variables  

RawStoria2 <- merge(x=RawStoriaDup, y=RawStoriaDup,  by = c("user_id","city_id","category_id","price"))

save(RawStoria2,file="RawStoria2.Rdata")
load("RawStoria2.Rdata")

#split ad ids in a new column as vector to order them 

RawStoria2$new <- paste(RawStoria2$id.x, RawStoria2$id.y, sep=",")

RawStoria2$new2 <- unique(strsplit(RawStoria2$new, ","))

#Allign vector ids 

RawStoria2$new3 <- lapply(RawStoria2$new2, FUN=function(x) x[order(x)])

#Group by ad id (tests) 

#RawStradia2$new5[[1]][2]
#RawStradia2$new5[[93]][2]
#RawStradia2$new5[[93]][1]

#Group by using rowid (to find duplicates)  

RawStoria2$rowid <- seq(1, nrow(RawStoria2),1)
#View(RawStradia2)

#after order/group by, put ad ids splitted as columns again to cut duplicates 

RawStoria2$new4 <- lapply(RawStoria2$new3, FUN=function(x) x[1])
RawStoria2$new5 <- lapply(RawStoria2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStoria2$new4 <- as.numeric(RawStoria2$new4)
RawStoria2$new5 <- as.numeric(RawStoria2$new5)

dfmin <- RawStoria2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStoria3 <- RawStoria2[RawStoria2$rowid %in% dfmin$minRowid, ]

Storiadupfinal <- RawStoria3[RawStoria3$new4!=RawStoria3$new5, ]

#find title similarity 

Storiadupfinal <- Storiadupfinal %>% mutate(similarity =as.numeric(stringsim(Storiadupfinal$title.x,Storiadupfinal$title.y)))

Storiadupfinal <- subset(Storiadupfinal,subset = similarity >0.7)

#remove duplicated ad ids based on y id 

Storiadupfinal2 <- Storiadupfinal[!(Storiadupfinal$id.x %in% Storiadupfinal$id.y), ]

Storiadupfinal2 <- subset(Storiadupfinal2, !duplicated(id.y))

#length(unique(Storiadupfinal2$id.y)) == nrow(Storiadupfinal2)

#Clean the data frame - Allign Columns and names 

Storiadupfinal2 <- Storiadupfinal2[c(1,2,3,4,7,8,15)]

colnames(Storiadupfinal2) <- c("user id", "city id","category id","price","ad id","title","similarity")

Storiadupfinal2 <- Storiadupfinal2[,c("ad id","user id","city id","category id","price","title","similarity")]

#save(Storiadupfinal,file="Storiadupfinal.Rdata")
save(Storiadupfinal2,file="Storiadupfinal2.Rdata")
#load("Storiadupfinal2.RData")



#create vec data to build the plot (using Date, Total active Ads by day and total dup)

vecdatastoria1 <- list(Sys.Date(),Storiadupfinal2, nrow(RawStoriaDup))
vecdatastoria2 <- list(Sys.Date(),Storiadupfinal2, nrow(RawStoriaDup))
vecdatastoria3 <- list(Sys.Date(),Storiadupfinal2, nrow(RawStoriaDup))

#load vec data by day

#save(vecdatastoria3,file="vecdatastoria3.RData")
#load("vecdatastoria1.RData")
#load("vecdatastoria2.RData")

#join vec data

joinvecstoria <- mapply(vecdatastoria1,vecdatastoria2,vecdatastoria3, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstoria[1]

yads <- joinvecstoria[3]

#count number of Duplicates rows 

duplicates1 <- sapply(vecdatastoria1[2], nrow)
duplicates2 <- sapply(vecdatastoria2[2], nrow)
duplicates3 <- sapply(vecdatastoria3[2], nrow)

#create data frame considering duplicates 

dfduplicates <- data.frame(c(duplicates1,duplicates2,duplicates3))

colnames(dfduplicates) <- ("Duplicates Ads")

#create final data frame with dates, total ads and duplicates 

df <- data.frame(unlist(xdate),unlist(yads),dfduplicates)

df$unlist.xdate. <- as.Date(df$unlist.xdate.,origin="1970-01-01")

colnames(df) <- c("Date","Ads","Duplicates")

#plot

ggplot(df, aes(Date)) + 
geom_bar(width=.6,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(40000, 340000)) + 
geom_text(aes(y= Duplicates,label = Duplicates, vjust=-2)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))

#save(df,file="dfstoriadup.RData")
#load("dfstoriadup.RData")

---------------stradia-------------------------------------------------------------------------------------

#save(RawStradiaDup,file="RawStradiaDup.RData")
#load("RawStradiaDup.RData")

#Ajust model (substring)

RawStradiaDup$model <- RawStradiaDup$model %>% str_replace(".*model","")
RawStradiaDup$model <- RawStradiaDup$model %>% str_replace("<=>","")


#Do the crossjoin for ad id and keep other variables 

RawStradia2 <- merge(x=RawStradiaDup, y=RawStradiaDup,  by = c("user_id","brand","model","year","mileage"))

#split ad ids in a new column as vector to order them 

RawStradia2$new <- paste(RawStradia2$id.x, RawStradia2$id.y, sep=",")

RawStradia2$new2 <- unique(strsplit(RawStradia2$new, ","))

#Allign vector ids 

RawStradia2$new3 <- lapply(RawStradia2$new2, FUN=function(x) x[order(x)])

#Group by using rowid  

RawStradia2$rowid <- seq(1, nrow(RawStradia2),1)

#after order/group by, put ad ids as splitted as columns again to cut duplicates 

RawStradia2$new4 <- lapply(RawStradia2$new3, FUN=function(x) x[1])
RawStradia2$new5 <- lapply(RawStradia2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStradia2$new4 <- as.numeric(RawStradia2$new4)
RawStradia2$new5 <- as.numeric(RawStradia2$new5)


dfmin <- RawStradia2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStradia3 <- RawStradia2[RawStradia2$rowid %in% dfmin$minRowid, ]

Stradiadupfinal <- RawStradia3[RawStradia3$new4!=RawStradia3$new5, ]

#remove duplicated ad ids based on y id 

Stradiadupfinal2 <- Stradiadupfinal[!(Stradiadupfinal$id.x %in% Stradiadupfinal$id.y), ]

Stradiadupfinal2 <- Stradiadupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiadupfinal2$description.x,Stradiadupfinal2$description.y)))

Stradiadupfinal2 <- subset(Stradiadupfinal2,subset = similarity >0.7)

Stradiadupfinal2 <- subset(Stradiadupfinal2, !duplicated(id.y), )

#length(unique(Stradiadupfinal2$id.y)) == nrow(Stradiadupfinal2)

#Clean the data frame - Allign Columns and names 

Stradiadupfinal3 <- Stradiadupfinal2[c(1,2,3,4,5,8,9,16)]

colnames(Stradiadupfinal3) <- c("user id", "brand","model","year","mileage","ad id","description","similarity")

Stradiadupfinal3 <- Stradiadupfinal3[,c("ad id","user id","brand","model","year","mileage","description","similarity")]


#save(Stradiadupfinal3,file="Stradiadupfinal3.RData")
#load("Stradiadupfinal3.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)

vecdatastradia3 <- list(Sys.Date(),Stradiadupfinal3, nrow(RawStradiaDup))

#load vec data by day

#save(vecdatastradia3,file="vecdatastradia3.RData")
load("vecdatastradia1.RData")
load("vecdatastradia2.RData")

#join vec data

joinvecstradia <- mapply(vecdatastradia1,vecdatastradia2,vecdatastradia3, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstradia[1]

yads <- joinvecstradia[3]

#count number of Duplicates rows 

duplicatestradia1 <- sapply(vecdatastradia1[2], nrow)
duplicatestradia2 <- sapply(vecdatastradia2[2], nrow)
duplicatestradia3 <- sapply(vecdatastradia3[2], nrow)

#create data frame considering duplicates 

dfduplicatestradia <- data.frame(c(duplicatestradia1,duplicatestradia2,duplicatestradia3))

colnames(dfduplicatestradia) <- ("Duplicates Ads")

#create final data frame 

dfstradia <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradia)

dfstradia$unlist.xdate. <- as.Date(dfstradia$unlist.xdate.,origin="1970-01-01")

colnames(dfstradia) <- c("Date","Ads","Duplicates")


#save(dfstradia,file="dfstradia.RData")
#load("dfstradia.RData")

#plot stradia

ggplot(dfstradia, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 55000)) + 
geom_text(aes(y= Duplicates,label = Duplicates, vjust=-1)) + 
geom_text(aes(y= Ads,label = Ads, vjust=2))



