#library 
#load package to connect to Mysql
library("RMySQL")
library("DBI")
library("dplyr")
library("data.table")
library("stringdist")
library("ggplot2")
library("reshape2")
#library("plyr")
library("formattable")
library("scales")
library("googleVis")
library("stringr")
library("xlsx")





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

#system("sudo killall ssh", wait=FALSE)

#cmd_storia <- 'ssh -i /Users/pedromatos/Documents/bi_id_rsa biuser@52.74.90.117 -p 10022 -L 10002:172.50.21.59:3306 -N'

#system(cmd_storia, wait=FALSE)

#cmd_storia <- 'C:/Jump/plink.exe -i C:/Jump/bi.ppk -N -batch  -ssh -L 1000:172.50.21.59:3306 biuser@52.74.90.117 -P 10022'

#cmd_stradia <- 'C:/Jump/plink.exe -i C:/Jump/id_rsa.ppk -N -batch  -ssh -L 10001:172.30.100.29:3306 inbi_r@134.213.201.219 -P 10022'

#wait 5 sec
#Sys.sleep(5)

#run plink command

#shell(cmd_storia, wait=FALSE)
#shell(cmd_stradia, wait=FALSE)

#wait 5 sec



# connect to Database

     #storia ID
conn_storia <-  dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3315)
     #Stradia IN 
conn_stradia <- dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3312)
     #Stradia LATAM 
conn_stradia_latam <- dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3311)


# Create SQL Statement

     #storia ID 
Storiadup <- "SELECT id, user_id, city_id, category_id, title,REPLACE(SUBSTRING_INDEX(params, '<br>price[currency]', 1), 'price<=>price<br>price<=>', '') as price FROM storiaid.ads WHERE status = 'active';"
     #Stradia IN
Stradiadup1 <- "SELECT COUNT(*) FROM cars_in.ads WHERE status = 'active';"

Stradiadup2 <- "SELECT id, user_id,SUBSTRING_INDEX(SUBSTRING_INDEX(description,'Variant: ', -1),':',1) as Variant,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,REPLACE(SUBSTRING_INDEX(params, '<br>version<=>', 1), 'make<=>', '') AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,REPLACE(SUBSTRING_INDEX(params, '<br>engine_power<=><br>', 1), 'color<=>','') as fuel FROM cars_in.ads WHERE status = 'active' AND description LIKE '%Variant%';"
#Stradiadup <- "SELECT id, user_id,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>version<=><br>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,REPLACE(SUBSTRING_INDEX(params, '<br>engine_power<=><br>', 1), 'color<=>','') as fuel, IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage,IF(INSTR(params, 'price<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>price<=>', -1), 15) AS DECIMAL(10)) , '?') as price FROM cars_in.ads WHERE status = 'active';"

    #Stradia Peru 
Stradiadupe1 <- "SELECT COUNT(*) FROM stradia_pe.ads WHERE status = 'active';"

Stradiadupe2 <- "SELECT id, user_id,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>year<=>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,REPLACE(SUBSTRING_INDEX(params, '<br>engine_power<=><br>', 1), 'color<=>','') as fuel,IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage FROM stradia_pe.ads WHERE status = 'active';"

    #Stradia Argentina 
Stradiadupar1 <- "SELECT COUNT(*) FROM stradia_ar.ads WHERE status = 'active';"

Stradiadupar2 <- "SELECT id, user_id,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>year<=>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,REPLACE(SUBSTRING_INDEX(params, '<br>engine_power<=><br>', 1), 'color<=>','') as fuel,IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage FROM stradia_ar.ads WHERE status = 'active';"


    #Stradia Ecuador 
Stradiadupec1 <- "SELECT COUNT(*) FROM stradia_ec.ads WHERE status = 'active';"

Stradiadupec2 <- "SELECT id, user_id,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>year<=>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,REPLACE(SUBSTRING_INDEX(params, '<br>engine_power<=><br>', 1), 'color<=>','') as fuel,IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage FROM stradia_ec.ads WHERE status = 'active';"


    #Stradia Colombia 
Stradiadupco1 <- "SELECT COUNT(*) FROM stradia_co.ads WHERE status = 'active';"

Stradiadupco2 <- "SELECT id, user_id,REPLACE(SUBSTRING_INDEX(params, '<br>model<=>', 1), 'make<=>', '') as brand,LEFT(params, LOCATE('<br>year<=>', params) -1) AS model,IF(INSTR(params, '<br>year<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>year<=>', -1), 15) AS DECIMAL(0)) , '?') as year,LEFT(params, LOCATE('<br>year<=>', params) -1) as fuel,IF(INSTR(params, 'mileage<=>') > 0, CAST(LEFT(SUBSTRING_INDEX(params, '<br>mileage<=>', -1), 15) AS DECIMAL(0)) , '?') as mileage FROM stradia_co.ads WHERE status = 'active';"
                                                   


#Get Queries

#Storia IND
RawStoriaDup <- dbGetQuery(conn_storia,Storiadup)

#Stradia IN 
RawStradiaDup1 <- dbGetQuery(conn_stradia,Stradiadup1)
RawStradiaDup <- dbGetQuery(conn_stradia,Stradiadup2)

#Stradia PE 
RawStradiaDupe1 <- dbGetQuery(conn_stradia_latam,Stradiadupe1)
RawStradiaDupe <- dbGetQuery(conn_stradia_latam,Stradiadupe2)

#Stradia AR
RawStradiaDupar1 <- dbGetQuery(conn_stradia_latam,Stradiadupar1)
RawStradiaDupar <- dbGetQuery(conn_stradia_latam,Stradiadupar2)

#Stradia EC
RawStradiaDupec1 <- dbGetQuery(conn_stradia_latam,Stradiadupec1)
RawStradiaDupec <- dbGetQuery(conn_stradia_latam,Stradiadupec2)

#Stradia CO
RawStradiaDupco1 <- dbGetQuery(conn_stradia_latam,Stradiadupco1)
RawStradiaDupco <- dbGetQuery(conn_stradia_latam,Stradiadupco2)



dbDisconnect(conn_storia)
dbDisconnect(conn_stradia)
dbDisconnect(conn_stradia_latam)


##Find duplications 

------------storia--------------------------------------------------------------------------------------------

#load Raw Data (Total Ads by day + Duplication by day)

#load("RawStoriaDup.RData")
save(RawStoriaDup,file="RawStoriaDup.RData")
#save(RawStoriafinal,file="RawStoriafinal.RData")


#Do the crossjoin for ad id and keep other variables  

RawStoria2 <- merge(x=RawStoriaDup, y=RawStoriaDup,  by = c("user_id","city_id","category_id","price"))

#save(RawStoria2,file="RawStoria2.Rdata")
#load("RawStoria2.Rdata")

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

save(Storiadupfinal2,file="Storiadupfinal.RData")
#load("Storiadupfinal.RData")

write.csv(Storiadupfinal2,file = "Storiadupfinal.csv",row.names=FALSE)


#create vec data to build the plot (using Date, Total active Ads by day and total dup)

vecdatastoria2 <- list(Sys.Date(),Storiadupfinal2, nrow(RawStoriaDup))

#load vec data by day

save(vecdatastoria2,file="vecdatastoria2.RData")

load("vecdatastoria1.RData")
load("vecdatastoria45.RData")
load("vecdatastoria46.RData")
load("vecdatastoria47.RData")
load("vecdatastoria48.RData")
load("vecdatastoria49.RData")
load("vecdatastoria50.RData")
load("vecdatastoria51.RData")
load("vecdatastoria52.RData")
load("vecdatastoria53.RData")
load("vecdatastoria54.RData")
load("vecdatastoria55.RData")
load("vecdatastoria56.RData")
load("vecdatastoria57.RData")
load("vecdatastoria58.RData")
load("vecdatastoria59.RData")
load("vecdatastoria60.RData")
load("vecdatastoria61.RData")
load("vecdatastoria62.RData")
load("vecdatastoria63.RData")
load("vecdatastoria64.RData")
load("vecdatastoria65.RData")
load("vecdatastoria66.RData")
load("vecdatastoria67.RData")
load("vecdatastoria68.RData")
load("vecdatastoria69.RData")
load("vecdatastoria70.RData")
load("vecdatastoria71.RData")





#join vec data

joinvecstoria <- mapply(vecdatastoria44,vecdatastoria45,vecdatastoria46,vecdatastoria47,vecdatastoria48,vecdatastoria49,vecdatastoria50,vecdatastoria51,vecdatastoria52,vecdatastoria53,vecdatastoria54,vecdatastoria55,vecdatastoria56,vecdatastoria57,vecdatastoria58,vecdatastoria59,vecdatastoria60,vecdatastoria61,vecdatastoria62,vecdatastoria63,vecdatastoria64,vecdatastoria65,vecdatastoria66,vecdatastoria67,vecdatastoria68,vecdatastoria69,vecdatastoria70,vecdatastoria71, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstoria[1]

yads <- joinvecstoria[3]

#count number of Duplicates rows 


duplicates44 <- sapply(vecdatastoria44[2], nrow)
duplicates45 <- sapply(vecdatastoria45[2], nrow)
duplicates46 <- sapply(vecdatastoria46[2], nrow)
duplicates47 <- sapply(vecdatastoria47[2], nrow)
duplicates48 <- sapply(vecdatastoria48[2], nrow)
duplicates49 <- sapply(vecdatastoria49[2], nrow)
duplicates50 <- sapply(vecdatastoria50[2], nrow)
duplicates51 <- sapply(vecdatastoria51[2], nrow)
duplicates52 <- sapply(vecdatastoria52[2], nrow)
duplicates53 <- sapply(vecdatastoria53[2], nrow)
duplicates54 <- sapply(vecdatastoria54[2], nrow)
duplicates55 <- sapply(vecdatastoria55[2], nrow)
duplicates56 <- sapply(vecdatastoria56[2], nrow)
duplicates57 <- sapply(vecdatastoria57[2], nrow)
duplicates58 <- sapply(vecdatastoria58[2], nrow)
duplicates59 <- sapply(vecdatastoria59[2], nrow)
duplicates60 <- sapply(vecdatastoria60[2], nrow)
duplicates61 <- sapply(vecdatastoria61[2], nrow)
duplicates62 <- sapply(vecdatastoria62[2], nrow)
duplicates63 <- sapply(vecdatastoria63[2], nrow)
duplicates64 <- sapply(vecdatastoria64[2], nrow)
duplicates65 <- sapply(vecdatastoria65[2], nrow)
duplicates66 <- sapply(vecdatastoria66[2], nrow)
duplicates67 <- sapply(vecdatastoria67[2], nrow)
duplicates68 <- sapply(vecdatastoria68[2], nrow)
duplicates69 <- sapply(vecdatastoria69[2], nrow)
duplicates70 <- sapply(vecdatastoria70[2], nrow)
duplicates71 <- sapply(vecdatastoria71[2], nrow)



#create data frame considering duplicates 

dfduplicates <- data.frame(c(duplicates44,duplicates45,duplicates46,duplicates47,duplicates48,duplicates49,duplicates50,duplicates51,duplicates52,duplicates53,duplicates54,duplicates55,duplicates56,duplicates57,duplicates58,duplicates59,duplicates60,duplicates61,duplicates62,duplicates63,duplicates64,duplicates65,duplicates66,duplicates67,duplicates68,duplicates69,duplicates70,duplicates71))

colnames(dfduplicates) <- ("Duplicates Ads")

#create final data frame with dates, total ads and duplicates 

df <- data.frame(unlist(xdate),unlist(yads),dfduplicates)

df$unlist.xdate. <- as.Date(df$unlist.xdate.,origin="1970-01-01")

colnames(df) <- c("Date","Ads","Duplicates")


#storia2906 <- data.frame(Date = as.Date("2017-06-30"),"Ads" = as.numeric(549510), "Duplicates" = as.numeric(100756))
storia0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(549661), "Duplicates" = as.numeric(100784))
storia0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(550240), "Duplicates" = as.numeric(100920))
storia0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(553405), "Duplicates" = as.numeric(107543))

df <- rbind(df,storia0107,storia0307,storia0807)

df <- df[order(as.Date(df$Date, format="%d/%m/%Y")),]

df$"perduplicates" <- percent(round(df$Duplicates/df$Ads,3))



#plot

options(scipen=10000)

ggplot(df, aes(Date)) + 
geom_bar(width=.6,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 600000)) + 
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=0,angle=90))



save(df,file="dfstoriadup.RData")
load("dfstoriadup.RData")

---------------stradia IN-------------------------------------------------------------------------------------

save(RawStradiaDup,file="RawStradiaDup.RData")
load("RawStradiaDup.RData")

# RawStradiaDup$mileage <- as.numeric(RawStradiaDup$mileage)
# 
# RawStradiaDup$mileage <- round(RawStradiaDup$mileage, -2)
# 
# RawStradiaDup$price <- as.numeric(RawStradiaDup$price)
# 
# RawStradiaDup$price <- round(RawStradiaDup$price, -4)

#Ajust model (substring)

RawStradiaDup$model <- RawStradiaDup$model %>% str_replace(".*model","")
RawStradiaDup$model <- RawStradiaDup$model %>% str_replace("<=>","")

RawStradiaDup$fuel <- RawStradiaDup$fuel %>% str_replace(".*fuel_type","")
RawStradiaDup$fuel <- RawStradiaDup$fuel %>% str_replace("<br>.*","")
RawStradiaDup$fuel <- RawStradiaDup$fuel %>% str_replace("<=>","")


RawStradiaDup$Variant <- RawStradiaDup$Variant %>% str_replace(".*Vehicle Type","")
#RawStradiaDup$Variant <- RawStradiaDup$Variant %>% str_replace(".*","")


#Do the crossjoin for ad id and keep other variables 

RawStradia2 <- merge(x=RawStradiaDup, y=RawStradiaDup,  by = c("user_id","brand","model","year","fuel","Variant"))


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

#Stradiadupfinal2 <- Stradiadupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiadupfinal2$description.x,Stradiadupfinal2$description.y)))

#Stradiadupfinal2 <- subset(Stradiadupfinal2,subset = similarity >0.7)

Stradiadupfinal2 <- subset(Stradiadupfinal2, !duplicated(id.y), )

#length(unique(Stradiadupfinal2$id.y)) == nrow(Stradiadupfinal2)

#Clean the data frame - Allign Columns and names 

Stradiadupfinal3 <- Stradiadupfinal2[c(1,2,3,4,5,6,8)]

colnames(Stradiadupfinal3) <- c("user id", "brand","model","year","fuel","Variant","ad id")

Stradiadupfinal3 <- Stradiadupfinal3[,c("ad id","user id","brand","model","year","fuel","Variant")]

#length(unique(Stradiadupfinal2$new5)) == nrow(Stradiadupfinal2)

save(Stradiadupfinal3,file="Stradiadupfinal.RData")
load("Stradiadupfinal.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)


vecdatastradia2 <- list(Sys.Date(),Stradiadupfinal3, RawStradiaDup1)

#load vec data by day

save(vecdatastradia2,file="vecdatastradia2.RData")




load("vecdatastradia1.RData")
load("vecdatastradia45.RData")
load("vecdatastradia45.RData")
load("vecdatastradia46.RData")
load("vecdatastradia47.RData")
load("vecdatastradia48.RData")
load("vecdatastradia49.RData")
load("vecdatastradia50.RData")
load("vecdatastradia51.RData")
load("vecdatastradia52.RData")
load("vecdatastradia53.RData")
load("vecdatastradia54.RData")
load("vecdatastradia55.RData")
load("vecdatastradia56.RData")
load("vecdatastradia57.RData")
load("vecdatastradia58.RData")
load("vecdatastradia59.RData")
load("vecdatastradia60.RData")
load("vecdatastradia61.RData")
load("vecdatastradia62.RData")
load("vecdatastradia63.RData")
load("vecdatastradia64.RData")
load("vecdatastradia65.RData")
load("vecdatastradia66.RData")
load("vecdatastradia67.RData")
load("vecdatastradia68.RData")
load("vecdatastradia69.RData")
load("vecdatastradia70.RData")
load("vecdatastradia71.RData")



#join vec data

joinvecstradia <- mapply(vecdatastradia44,vecdatastradia45,vecdatastradia46,vecdatastradia47,vecdatastradia48,vecdatastradia49,vecdatastradia50,vecdatastradia51,vecdatastradia52,vecdatastradia53,vecdatastradia54,vecdatastradia55,vecdatastradia56,vecdatastradia57,vecdatastradia58,vecdatastradia59,vecdatastradia60,vecdatastradia61,vecdatastradia62,vecdatastradia63,vecdatastradia64,vecdatastradia65,vecdatastradia66,vecdatastradia67,vecdatastradia68,vecdatastradia69,vecdatastradia70,vecdatastradia71, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 


xdate <- joinvecstradia[1]

yads <- joinvecstradia[3]

#count number of Duplicates rows 

duplicatestradia44 <- sapply(vecdatastradia44[2], nrow)
duplicatestradia45 <- sapply(vecdatastradia45[2], nrow)
duplicatestradia46 <- sapply(vecdatastradia46[2], nrow)
duplicatestradia47 <- sapply(vecdatastradia47[2], nrow)
duplicatestradia48 <- sapply(vecdatastradia48[2], nrow)
duplicatestradia49 <- sapply(vecdatastradia49[2], nrow)
duplicatestradia50 <- sapply(vecdatastradia50[2], nrow)
duplicatestradia51 <- sapply(vecdatastradia51[2], nrow)
duplicatestradia52 <- sapply(vecdatastradia52[2], nrow)
duplicatestradia53 <- sapply(vecdatastradia53[2], nrow)
duplicatestradia54 <- sapply(vecdatastradia54[2], nrow)
duplicatestradia55 <- sapply(vecdatastradia55[2], nrow)
duplicatestradia56 <- sapply(vecdatastradia56[2], nrow)
duplicatestradia57 <- sapply(vecdatastradia57[2], nrow)
duplicatestradia58 <- sapply(vecdatastradia58[2], nrow)
duplicatestradia59 <- sapply(vecdatastradia59[2], nrow)
duplicatestradia60 <- sapply(vecdatastradia60[2], nrow)
duplicatestradia61 <- sapply(vecdatastradia61[2], nrow)
duplicatestradia62 <- sapply(vecdatastradia62[2], nrow)
duplicatestradia63 <- sapply(vecdatastradia63[2], nrow)
duplicatestradia64 <- sapply(vecdatastradia64[2], nrow)
duplicatestradia65 <-  sapply(vecdatastradia65[2], nrow)
duplicatestradia66 <- sapply(vecdatastradia66[2], nrow)
duplicatestradia67 <- sapply(vecdatastradia67[2], nrow)
duplicatestradia68 <- sapply(vecdatastradia68[2], nrow)
duplicatestradia69 <-  sapply(vecdatastradia69[2], nrow)
duplicatestradia70 <- sapply(vecdatastradia70[2], nrow)
duplicatestradia71 <- sapply(vecdatastradia71[2], nrow)


#create data frame considering duplicates 

dfduplicatestradia <- data.frame(c(duplicatestradia44,duplicatestradia45,duplicatestradia46,duplicatestradia47,duplicatestradia48,duplicatestradia49,duplicatestradia50,duplicatestradia51,duplicatestradia52,duplicatestradia53,duplicatestradia54,duplicatestradia55,duplicatestradia56,duplicatestradia57,duplicatestradia58,duplicatestradia59,duplicatestradia60,duplicatestradia61,duplicatestradia62,duplicatestradia63,duplicatestradia64,duplicatestradia65,duplicatestradia66,duplicatestradia67,duplicatestradia68,duplicatestradia69,duplicatestradia70,duplicatestradia71))


colnames(dfduplicatestradia) <- ("Duplicates Ads")

#create final data frame 

dfstradia <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradia)

dfstradia$unlist.xdate. <- as.Date(dfstradia$unlist.xdate.,origin="1970-01-01")

colnames(dfstradia) <- c("Date","Ads","Duplicates")

stradia0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(53470), "Duplicates" = as.numeric(2134))
stradia0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(53525), "Duplicates" = as.numeric(2070))
stradia0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(55240), "Duplicates" = as.numeric(1050))

dfstradia <- rbind(dfstradia,stradia0107,stradia0307,stradia0807)


dfstradia <- dfstradia[order(as.Date(dfstradia$Date, format="%d/%m/%Y")),]

dfstradia$"perduplicates" <- percent(round(dfstradia$Duplicates/dfstradia$Ads,3))

rownames(dfstradia) <- NULL

save(dfstradia,file="dfstradia.RData")
load("dfstradia.RData")

#plot stradia

options(scipen=10000)

ggplot(dfstradia, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 60000)) + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))

----------stradia LATAM------------------------------------------------------------------------------------------------------------------------------------------

Argentina ------------------------------------------------------

save(RawStradiaDupar,file="RawStradiaDupar.RData")
load("RawStradiaDupar.RData")

------Adjust SUBSTRING--------

RawStradiaDupar$model <- RawStradiaDupar$model %>% str_replace(".*model","")
RawStradiaDupar$model <- RawStradiaDupar$model %>% str_replace("<=>","")
                                                   
RawStradiaDupar$fuel <- RawStradiaDupar$fuel %>% str_replace(".*fuel_type<=>","")
RawStradiaDupar$fuel <- RawStradiaDupar$fuel %>% str_replace("<br>.*","")
                                                   


#Do the crossjoin for ad id and keep other variables 

RawStradiar2 <- merge(x=RawStradiaDupar, y=RawStradiaDupar,  by = c("user_id","brand","model","year","fuel","mileage"))


#split ad ids in a new column as vector to order them 

RawStradiar2$new <- paste(RawStradiar2$id.x, RawStradiar2$id.y, sep=",")

RawStradiar2$new2 <- unique(strsplit(RawStradiar2$new, ","))

#Allign vector ids 

RawStradiar2$new3 <- lapply(RawStradiar2$new2, FUN=function(x) x[order(x)])

#Group by using rowid  

RawStradiar2$rowid <- seq(1, nrow(RawStradiar2),1)

#after order/group by, put ad ids as splitted as columns again to cut duplicates 

RawStradiar2$new4 <- lapply(RawStradiar2$new3, FUN=function(x) x[1])
RawStradiar2$new5 <- lapply(RawStradiar2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStradiar2$new4 <- as.numeric(RawStradiar2$new4)
RawStradiar2$new5 <- as.numeric(RawStradiar2$new5)


dfminar <- RawStradiar2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStradiar3 <- RawStradiar2[RawStradiar2$rowid %in% dfminar$minRowid, ]

Stradiardupfinal <- RawStradiar3[RawStradiar3$new4!=RawStradiar3$new5, ]

#remove duplicated ad ids based on y id 

Stradiardupfinal2 <- Stradiardupfinal[!(Stradiardupfinal$id.x %in% Stradiardupfinal$id.y), ]

#Stradiardupfinal2 <- Stradiardupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiardupfinal2$description.x,Stradiardupfinal2$description.y)))

#Stradiardupfinal2 <- subset(Stradiardupfinal2,subset = similarity >0.7)

Stradiardupfinal2 <- subset(Stradiardupfinal2, !duplicated(id.y), )


#Clean the data frame - Allign Columns and names 

Stradiardupfinal3 <- Stradiardupfinal2[c(1,2,3,4,5,6,8)]

colnames(Stradiardupfinal3) <- c("user id", "brand","model","year","fuel","mileage","ad id")

Stradiardupfinal3 <- Stradiardupfinal3[,c("ad id","user id","brand","model","year","fuel","mileage")]


save(Stradiardupfinal3,file="Stradiardupfinal.RData")
load("Stradiardupfinal.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)


vecdatastradiar2 <- list(Sys.Date(),Stradiardupfinal3, RawStradiaDupar1)

#load vec data by day

save(vecdatastradiar2,file="vecdatastradiar2.RData")



load("vecdatastradiar1.RData")
load("vecdatastradiar2.RData")

#join vec data

joinvecstradia <- mapply(vecdatastradiar1,vecdatastradiar2, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstradia[1]

yads <- joinvecstradia[3]


#count number of Duplicates rows 


duplicatestradiar1 <- sapply(vecdatastradiar1[2], nrow)
duplicatestradiar2 <- sapply(vecdatastradiar2[2], nrow)


#create data frame considering duplicates 

dfduplicatestradiar <- data.frame(c(duplicatestradiar1,duplicatestradiar2))



colnames(dfduplicatestradiar) <- ("Duplicates Ads")

#create final data frame 

dfstradiar <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradiar)

dfstradiar$unlist.xdate. <- as.Date(dfstradiar$unlist.xdate.,origin="1970-01-01")

colnames(dfstradiar) <- c("Date","Ads","Duplicates")

# stradiar0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(5945), "Duplicates" = as.numeric(756))
# stradiar0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(5940), "Duplicates" = as.numeric(757))
# stradiar0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(5942), "Duplicates" = as.numeric(164))
# 
# 
# dfstradiar <- rbind(dfstradiar,stradiar0107,stradiar0307,stradiar0807)

dfstradiar <- dfstradiar[order(as.Date(dfstradiar$Date, format="%d/%m/%Y")),]

dfstradiar$"perduplicates" <- percent(round(dfstradiar$Duplicates/dfstradiar$Ads,3))

rownames(dfstradiar) <- NULL

save(dfstradiar,file="dfstradiar.RData")
load("dfstradiar.RData")



#plot stradia

options(scipen=10000)

ggplot(dfstradiar, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 9000)) + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))

class(dfstradiar$perduplicates_)

test <- gvisComboChart(dfstradiar,
xvar = 'Date', yvar = c('Ads','perduplicates'),
  options=list(seriesType="bars",
                                     series='{1: {type:"line"}}'))

plot(test)

dfstradiar$perduplicates_ <- as.numeric(sub("%","",dfstradiar$perduplicates))

test <- gvisComboChart(dfstradiar, xvar="Date", yvar=c("Ads", "perduplicates_"),
                options=list(title="Duplicates",
titleTextStyle="{color:'red',
  fontName:'Courier',
  fontSize:16}",
curveType="function", 
pointSize=9,
series="[{targetAxisIndex:0, 
  color:'red'}, 
  {targetAxisIndex:1,
    color:'blue'}]",
vAxes="[{title:'perduplicates_',
  titleTextStyle: {color: 'blue'},
  textStyle:{color: 'blue'},
  textPosition: 'out'}, 
  {title:'Ads',
    titleTextStyle: {color: 'red'},  
    textStyle:{color: 'red'},
    textPosition: 'out'}]",
hAxes="[{title:'Date',
  textPosition: 'out'}]",
width=550, height=500
),
chartid="twoaxislinechart"
)

plot(test)




-------------------PERU-------------------------------------------------------------------------------------------------


save(RawStradiaDupe,file="RawStradiaDupe.RData")
load("RawStradiaDupe.RData")

------Adjust SUBSTRING--------

RawStradiaDupe$model <- RawStradiaDupe$model %>% str_replace(".*model","")
RawStradiaDupe$model <- RawStradiaDupe$model %>% str_replace("<=>","")

RawStradiaDupe$fuel <- RawStradiaDupe$fuel %>% str_replace(".*fuel_type","")
RawStradiaDupe$fuel <- RawStradiaDupe$fuel %>% str_replace("<br>.*","")
RawStradiaDupe$fuel <- RawStradiaDupe$fuel %>% str_replace("<=>","")


#Do the crossjoin for ad id and keep other variables 

RawStradiape2 <- merge(x=RawStradiaDupe, y=RawStradiaDupe,  by = c("user_id","brand","model","year","fuel","mileage"))


#split ad ids in a new column as vector to order them 

RawStradiape2$new <- paste(RawStradiape2$id.x, RawStradiape2$id.y, sep=",")

RawStradiape2$new2 <- unique(strsplit(RawStradiape2$new, ","))

#Allign vector ids 

RawStradiape2$new3 <- lapply(RawStradiape2$new2, FUN=function(x) x[order(x)])

#Group by using rowid  

RawStradiape2$rowid <- seq(1, nrow(RawStradiape2),1)

#after order/group by, put ad ids as splitted as columns again to cut duplicates 

RawStradiape2$new4 <- lapply(RawStradiape2$new3, FUN=function(x) x[1])
RawStradiape2$new5 <- lapply(RawStradiape2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStradiape2$new4 <- as.numeric(RawStradiape2$new4)
RawStradiape2$new5 <- as.numeric(RawStradiape2$new5)


dfminpe <- RawStradiape2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStradiape3 <- RawStradiape2[RawStradiape2$rowid %in% dfminpe$minRowid, ]

Stradiapedupfinal <- RawStradiape3[RawStradiape3$new4!=RawStradiape3$new5, ]

#remove duplicated ad ids based on y id 

Stradiapedupfinal2 <- Stradiapedupfinal[!(Stradiapedupfinal$id.x %in% Stradiapedupfinal$id.y), ]

#Stradiapedupfinal2 <- Stradiapedupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiapedupfinal2$description.x,Stradiapedupfinal2$description.y)))

#Stradiapedupfinal2 <- subset(Stradiapedupfinal2,subset = similarity >0.7)

Stradiapedupfinal2 <- subset(Stradiapedupfinal2, !duplicated(id.y), )


#Clean the data frame - Allign Columns and names 

Stradiapedupfinal3 <- Stradiapedupfinal2[c(1,2,3,4,5,6,8)]

colnames(Stradiapedupfinal3) <- c("user id", "brand","model","year","fuel","mileage","ad id")

Stradiapedupfinal3 <- Stradiapedupfinal3[,c("ad id","user id","brand","model","year","fuel","mileage")]


save(Stradiapedupfinal3,file="Stradiapedupfinal.RData")
load("Stradiapedupfinal.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)


vecdatastradiape2 <- list(Sys.Date(),Stradiapedupfinal3, RawStradiaDupe1)

#load vec data by day

save(vecdatastradiape2,file="vecdatastradiape2.RData")

load("vecdatastradiape1.RData")
load("vecdatastradiape44.RData")
load("vecdatastradiape45.RData")
load("vecdatastradiape46.RData")
load("vecdatastradiape47.RData")
load("vecdatastradiape48.RData")
load("vecdatastradiape49.RData")
load("vecdatastradiape50.RData")
load("vecdatastradiape51.RData")
load("vecdatastradiape52.RData")
load("vecdatastradiape53.RData")
load("vecdatastradiape54.RData")
load("vecdatastradiape55.RData")
load("vecdatastradiape56.RData")
load("vecdatastradiape57.RData")
load("vecdatastradiape58.RData")
load("vecdatastradiape59.RData")
load("vecdatastradiape60.RData")
load("vecdatastradiape61.RData")
load("vecdatastradiape62.RData")
load("vecdatastradiape63.RData")
load("vecdatastradiape64.RData")
load("vecdatastradiape65.RData")
load("vecdatastradiape66.RData")
load("vecdatastradiape67.RData")
load("vecdatastradiape68.RData")
load("vecdatastradiape69.RData")
load("vecdatastradiape70.RData")


#join vec data

joinvecstradiape <- mapply(vecdatastradiape43,vecdatastradiape44,vecdatastradiape45,vecdatastradiape46,vecdatastradiape47,vecdatastradiape48,vecdatastradiape49,vecdatastradiape50,vecdatastradiape51,vecdatastradiape52,vecdatastradiape53,vecdatastradiape54,vecdatastradiape55,vecdatastradiape56,vecdatastradiape57,vecdatastradiape58,vecdatastradiape59,vecdatastradiape60,vecdatastradiape61,vecdatastradiape62,vecdatastradiape63,vecdatastradiape64,vecdatastradiape65,vecdatastradiape66,vecdatastradiape67,vecdatastradiape69,vecdatastradiape70, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstradiape[1]
yads <- joinvecstradiape[3]


#count number of Duplicates rows 

duplicatestradiape43 <- sapply(vecdatastradiape43[2], nrow)
duplicatestradiape44 <- sapply(vecdatastradiape44[2], nrow)
duplicatestradiape45 <- sapply(vecdatastradiape45[2], nrow)
duplicatestradiape46 <- sapply(vecdatastradiape46[2], nrow)
duplicatestradiape47 <- sapply(vecdatastradiape47[2], nrow)
duplicatestradiape48 <- sapply(vecdatastradiape48[2], nrow)
duplicatestradiape49 <- sapply(vecdatastradiape49[2], nrow)
duplicatestradiape50 <- sapply(vecdatastradiape50[2], nrow)
duplicatestradiape51 <- sapply(vecdatastradiape51[2], nrow)
duplicatestradiape52 <- sapply(vecdatastradiape52[2], nrow)
duplicatestradiape53 <- sapply(vecdatastradiape53[2], nrow)
duplicatestradiape54 <- sapply(vecdatastradiape54[2], nrow)
duplicatestradiape55 <- sapply(vecdatastradiape55[2], nrow)
duplicatestradiape56 <- sapply(vecdatastradiape56[2], nrow)
duplicatestradiape57 <- sapply(vecdatastradiape57[2], nrow)
duplicatestradiape58 <- sapply(vecdatastradiape58[2], nrow)
duplicatestradiape59 <- sapply(vecdatastradiape59[2], nrow)
duplicatestradiape60 <- sapply(vecdatastradiape60[2], nrow)
duplicatestradiape61 <- sapply(vecdatastradiape61[2], nrow)
duplicatestradiape62 <- sapply(vecdatastradiape62[2], nrow)
duplicatestradiape63 <- sapply(vecdatastradiape63[2], nrow)
duplicatestradiape64 <- sapply(vecdatastradiape64[2], nrow)
duplicatestradiape65 <- sapply(vecdatastradiape65[2], nrow)
duplicatestradiape66 <- sapply(vecdatastradiape66[2], nrow)
duplicatestradiape67 <- sapply(vecdatastradiape67[2], nrow)
duplicatestradiape69 <- sapply(vecdatastradiape69[2], nrow)
duplicatestradiape70 <- sapply(vecdatastradiape70[2], nrow)



#create data frame considering duplicates 

dfduplicatestradiape <- data.frame(c(duplicatestradiape43,duplicatestradiape44,duplicatestradiape45,duplicatestradiape46,duplicatestradiape47,duplicatestradiape48,duplicatestradiape49,duplicatestradiape50,duplicatestradiape51,duplicatestradiape52,duplicatestradiape53,duplicatestradiape54,duplicatestradiape55,duplicatestradiape56,duplicatestradiape57,duplicatestradiape58,duplicatestradiape59,duplicatestradiape60,duplicatestradiape61,duplicatestradiape62,duplicatestradiape63,duplicatestradiape64,duplicatestradiape65,duplicatestradiape66,duplicatestradiape67,duplicatestradiape69,duplicatestradiape70))



colnames(dfduplicatestradiape) <- ("Duplicates Ads")

#create final data frame 

dfstradiape <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradiape)

dfstradiape$unlist.xdate. <- as.Date(dfstradiape$unlist.xdate.,origin="1970-01-01")

colnames(dfstradiape) <- c("Date","Ads","Duplicates")

stradiape0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(112), "Duplicates" = as.numeric(6))
stradiape0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(94), "Duplicates" = as.numeric(5))
stradiape0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(82), "Duplicates" = as.numeric(4))
stradiape2907 <- data.frame(Date = as.Date("2017-07-29"),"Ads" = as.numeric(324), "Duplicates" = as.numeric(41))



dfstradiape <- rbind(dfstradiape,stradiape0107,stradiape0307,stradiape0807,stradiape2907)

dfstradiape <- dfstradiape[order(as.Date(dfstradiape$Date, format="%d/%m/%Y")),]

dfstradiape$"perduplicates" <- percent(round(dfstradiape$Duplicates/dfstradiape$Ads,3))

rownames(dfstradiape) <- NULL

save(dfstradiape,file="dfstradiape.RData")
load("dfstradiape.RData")

#plot stradia

ggplot(dfstradiape, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 400)) + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))


-------------------------------------Colombia---------------------------------------------------------------------------------------------


save(RawStradiaDupco,file="RawStradiaDupco.RData")
load("RawStradiaDupco.RData")


------Adjust SUBSTRING--------

RawStradiaDupco$model <- RawStradiaDupco$model %>% str_replace(".*model","")
RawStradiaDupco$model <- RawStradiaDupco$model %>% str_replace("<=>","")

RawStradiaDupco$fuel <- RawStradiaDupco$fuel %>% str_replace(".*fuel_type","")
RawStradiaDupco$fuel <- RawStradiaDupco$fuel %>% str_replace("<br>.*","")
RawStradiaDupco$fuel <- RawStradiaDupco$fuel %>% str_replace("<=>","")




#Do the crossjoin for ad id and keep other variables 

RawStradiaco2 <- merge(x=RawStradiaDupco, y=RawStradiaDupco,  by = c("user_id","brand","model","year","fuel","mileage"))


#split ad ids in a new column as vector to order them 

RawStradiaco2$new <- paste(RawStradiaco2$id.x, RawStradiaco2$id.y, sep=",")

RawStradiaco2$new2 <- unique(strsplit(RawStradiaco2$new, ","))

#Allign vector ids 

RawStradiaco2$new3 <- lapply(RawStradiaco2$new2, FUN=function(x) x[order(x)])

#Group by using rowid  

RawStradiaco2$rowid <- seq(1, nrow(RawStradiaco2),1)

#after order/group by, put ad ids as splitted as columns again to cut duplicates 

RawStradiaco2$new4 <- lapply(RawStradiaco2$new3, FUN=function(x) x[1])
RawStradiaco2$new5 <- lapply(RawStradiaco2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStradiaco2$new4 <- as.numeric(RawStradiaco2$new4)
RawStradiaco2$new5 <- as.numeric(RawStradiaco2$new5)


dfminco <- RawStradiaco2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStradiaco3 <- RawStradiaco2[RawStradiaco2$rowid %in% dfminco$minRowid, ]

Stradiacodupfinal <- RawStradiaco3[RawStradiaco3$new4!=RawStradiaco3$new5, ]

#remove duplicated ad ids based on y id 

Stradiacodupfinal2 <- Stradiacodupfinal[!(Stradiacodupfinal$id.x %in% Stradiacodupfinal$id.y), ]

#Stradiacodupfinal2 <- Stradiacodupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiacodupfinal2$description.x,Stradiacodupfinal2$description.y)))

#Stradiacodupfinal2 <- subset(Stradiacodupfinal2,subset = similarity >0.7)

Stradiacodupfinal2 <- subset(Stradiacodupfinal2, !duplicated(id.y), )


#Clean the data frame - Allign Columns and names 

Stradiacodupfinal3 <- Stradiacodupfinal2[c(1,2,3,4,5,6,8)]

colnames(Stradiacodupfinal3) <- c("user id", "brand","model","year","fuel","mileage","ad id")

Stradiacodupfinal3 <- Stradiacodupfinal3[,c("ad id","user id","brand","model","year","fuel","mileage")]


save(Stradiacodupfinal3,file="Stradiacodupfinal.RData")
load("Stradiacodupfinal.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)


vecdatastradiaco2 <- list(Sys.Date(),Stradiacodupfinal3, RawStradiaDupco1)


#load vec data by day

save(vecdatastradiaco2,file="vecdatastradiaco2.RData")


load("vecdatastradiaco1.RData")
load("vecdatastradiaco44.RData")
load("vecdatastradiaco45.RData")
load("vecdatastradiaco46.RData")
load("vecdatastradiaco47.RData")
load("vecdatastradiaco48.RData")
load("vecdatastradiaco49.RData")
load("vecdatastradiaco50.RData")
load("vecdatastradiaco51.RData")
load("vecdatastradiaco52.RData")
load("vecdatastradiaco53.RData")
load("vecdatastradiaco54.RData")
load("vecdatastradiaco55.RData")
load("vecdatastradiaco56.RData")
load("vecdatastradiaco57.RData")
load("vecdatastradiaco58.RData")
load("vecdatastradiaco59.RData")
load("vecdatastradiaco60.RData")
load("vecdatastradiaco61.RData")
load("vecdatastradiaco62.RData")
load("vecdatastradiaco63.RData")
load("vecdatastradiaco64.RData")
load("vecdatastradiaco65.RData")
load("vecdatastradiaco66.RData")
load("vecdatastradiaco67.RData")
load("vecdatastradiaco68.RData")
load("vecdatastradiaco69.RData")
load("vecdatastradiaco70.RData")


#join vec data

joinvecstradiaco <- mapply(vecdatastradiaco43,vecdatastradiaco44,vecdatastradiaco45,vecdatastradiaco46,vecdatastradiaco47,vecdatastradiaco48,vecdatastradiaco49,vecdatastradiaco50,vecdatastradiaco51,vecdatastradiaco52,vecdatastradiaco53,vecdatastradiaco54,vecdatastradiaco55,vecdatastradiaco56,vecdatastradiaco57,vecdatastradiaco58,vecdatastradiaco59,vecdatastradiaco60,vecdatastradiaco61,vecdatastradiaco62,vecdatastradiaco63,vecdatastradiaco64,vecdatastradiaco65,vecdatastradiaco66,vecdatastradiaco67,vecdatastradiaco68,vecdatastradiaco69,vecdatastradiaco70, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstradiaco[1]
yads <- joinvecstradiaco[3]


#count number of Duplicates rows 

duplicatestradiaco43 <- sapply(vecdatastradiaco43[2], nrow)
duplicatestradiaco44 <- sapply(vecdatastradiaco44[2], nrow)
duplicatestradiaco45 <- sapply(vecdatastradiaco45[2], nrow)
duplicatestradiaco46 <- sapply(vecdatastradiaco46[2], nrow)
duplicatestradiaco47 <- sapply(vecdatastradiaco47[2], nrow)
duplicatestradiaco48 <- sapply(vecdatastradiaco48[2], nrow)
duplicatestradiaco49 <- sapply(vecdatastradiaco49[2], nrow)
duplicatestradiaco50 <- sapply(vecdatastradiaco50[2], nrow)
duplicatestradiaco51 <- sapply(vecdatastradiaco51[2], nrow)
duplicatestradiaco52 <- sapply(vecdatastradiaco52[2], nrow)
duplicatestradiaco53 <- sapply(vecdatastradiaco53[2], nrow)
duplicatestradiaco54 <- sapply(vecdatastradiaco54[2], nrow)
duplicatestradiaco55 <- sapply(vecdatastradiaco55[2], nrow)
duplicatestradiaco56 <- sapply(vecdatastradiaco56[2], nrow)
duplicatestradiaco57 <- sapply(vecdatastradiaco57[2], nrow)
duplicatestradiaco58 <- sapply(vecdatastradiaco58[2], nrow)
duplicatestradiaco59 <- sapply(vecdatastradiaco59[2], nrow)
duplicatestradiaco60 <- sapply(vecdatastradiaco60[2], nrow)
duplicatestradiaco61 <- sapply(vecdatastradiaco61[2], nrow)
duplicatestradiaco62 <- sapply(vecdatastradiaco62[2], nrow)
duplicatestradiaco63 <- sapply(vecdatastradiaco63[2], nrow)
duplicatestradiaco64 <- sapply(vecdatastradiaco64[2], nrow)
duplicatestradiaco65 <- sapply(vecdatastradiaco65[2], nrow)
duplicatestradiaco66 <- sapply(vecdatastradiaco66[2], nrow)
duplicatestradiaco67 <- sapply(vecdatastradiaco67[2], nrow)
duplicatestradiaco68 <- sapply(vecdatastradiaco68[2], nrow)
duplicatestradiaco69 <- sapply(vecdatastradiaco69[2], nrow)
duplicatestradiaco70 <- sapply(vecdatastradiaco70[2], nrow)


#create data frame considering duplicates 

dfduplicatestradiaco <- data.frame(c(duplicatestradiaco43,duplicatestradiaco44,duplicatestradiaco45,duplicatestradiaco46,duplicatestradiaco47,duplicatestradiaco48,duplicatestradiaco49,duplicatestradiaco50,duplicatestradiaco51,duplicatestradiaco52,duplicatestradiaco53,duplicatestradiaco54,duplicatestradiaco55,duplicatestradiaco56,duplicatestradiaco57,duplicatestradiaco58,duplicatestradiaco59,duplicatestradiaco60,duplicatestradiaco61,duplicatestradiaco62,duplicatestradiaco63,duplicatestradiaco64,duplicatestradiaco65,duplicatestradiaco66,duplicatestradiaco67,duplicatestradiaco68,duplicatestradiaco69,duplicatestradiaco70))



colnames(dfduplicatestradiaco) <- ("Duplicates Ads")

#create final data frame 

dfstradiaco <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradiaco)

dfstradiaco$unlist.xdate. <- as.Date(dfstradiaco$unlist.xdate.,origin="1970-01-01")

colnames(dfstradiaco) <- c("Date","Ads","Duplicates")

stradiaco0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(4445), "Duplicates" = as.numeric(256))
stradiaco0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(4490), "Duplicates" = as.numeric(254))
stradiaco0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(5090), "Duplicates" = as.numeric(161))


dfstradiaco <- rbind(dfstradiaco,stradiaco0107,stradiaco0307,stradiaco0807)

dfstradiaco <- dfstradiaco[order(as.Date(dfstradiaco$Date, format="%d/%m/%Y")),]

dfstradiaco$"perduplicates" <- percent(round(dfstradiaco$Duplicates/dfstradiaco$Ads,3))

rownames(dfstradiaco) <- NULL

#dfstradiaco <- dfstradiaco[c(-1), ]

save(dfstradiaco,file="dfstradiaco.RData")
load("dfstradiaco.RData")

#plot stradia

ggplot(dfstradiaco, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 6000)) + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))


---------------ECUADOR----------------------------------------------------------------------------------------------------


save(RawStradiaDupec,file="RawStradiaDupec.RData")
load("RawStradiaDupec.RData")

------Adjust SUBSTRING--------

RawStradiaDupec$model <- RawStradiaDupec$model %>% str_replace(".*model","")
RawStradiaDupec$model <- RawStradiaDupec$model %>% str_replace("<=>","")

RawStradiaDupec$fuel <- RawStradiaDupec$fuel %>% str_replace(".*fuel_type<=>","")
RawStradiaDupec$fuel <- RawStradiaDupec$fuel %>% str_replace("<br>.*","")
RawStradiaDupec$fuel <- RawStradiaDupec$fuel %>% str_replace("<=>","")




#Do the crossjoin for ad id and keep other variables 

RawStradiaec2 <- merge(x=RawStradiaDupec, y=RawStradiaDupec,  by = c("user_id","brand","model","year","fuel","mileage"))


#split ad ids in a new column as vector to order them 

RawStradiaec2$new <- paste(RawStradiaec2$id.x, RawStradiaec2$id.y, sep=",")

RawStradiaec2$new2 <- unique(strsplit(RawStradiaec2$new, ","))

#Allign vector ids 

RawStradiaec2$new3 <- lapply(RawStradiaec2$new2, FUN=function(x) x[order(x)])

#Group by using rowid  

RawStradiaec2$rowid <- seq(1, nrow(RawStradiaec2),1)

#after order/group by, put ad ids as splitted as columns again to cut duplicates 

RawStradiaec2$new4 <- lapply(RawStradiaec2$new3, FUN=function(x) x[1])
RawStradiaec2$new5 <- lapply(RawStradiaec2$new3, FUN=function(x) x[2])

#transform these columns into numeric type

RawStradiaec2$new4 <- as.numeric(RawStradiaec2$new4)
RawStradiaec2$new5 <- as.numeric(RawStradiaec2$new5)


dfminec <- RawStradiaec2 %>% group_by(new4, new5) %>% summarise(minRowid = min(rowid))

RawStradiaec3 <- RawStradiaec2[RawStradiaec2$rowid %in% dfminec$minRowid, ]

Stradiaecdupfinal <- RawStradiaec3[RawStradiaec3$new4!=RawStradiaec3$new5, ]

#remove duplicated ad ids based on y id 

Stradiaecdupfinal2 <- Stradiaecdupfinal[!(Stradiaecdupfinal$id.x %in% Stradiaecdupfinal$id.y), ]

#Stradiaecdupfinal2 <- Stradiaecdupfinal2 %>% mutate(similarity =as.numeric(stringsim(Stradiaecdupfinal2$description.x,Stradiaecdupfinal2$description.y)))

#Stradiaecdupfinal2 <- subset(Stradiaecdupfinal2,subset = similarity >0.7)

Stradiaecdupfinal2 <- subset(Stradiaecdupfinal2, !duplicated(id.y), )


#Clean the data frame - Allign Columns and names 

Stradiaecdupfinal3 <- Stradiaecdupfinal2[c(1,2,3,4,5,6,8)]

colnames(Stradiaecdupfinal3) <- c("user id", "brand","model","year","fuel","mileage","ad id")

Stradiaecdupfinal3 <- Stradiaecdupfinal3[,c("ad id","user id","brand","model","year","fuel","mileage")]


save(Stradiaecdupfinal3,file="Stradiaecdupfinal.RData")
#load("Stradiaecdupfinal.RData")


#create vec data to build the plot (using Date, Total active Ads by day and total dup)


vecdatastradiaec2 <- list(Sys.Date(),Stradiaecdupfinal3, RawStradiaDupec1)

#load vec data by day

save(vecdatastradiaec2,file="vecdatastradiaec2.RData")


load("vecdatastradiaec1.RData")
load("vecdatastradiaec44.RData")
load("vecdatastradiaec45.RData")
load("vecdatastradiaec46.RData")
load("vecdatastradiaec47.RData")
load("vecdatastradiaec48.RData")
load("vecdatastradiaec49.RData")
load("vecdatastradiaec50.RData")
load("vecdatastradiaec51.RData")
load("vecdatastradiaec52.RData")
load("vecdatastradiaec53.RData")
load("vecdatastradiaec54.RData")
load("vecdatastradiaec55.RData")
load("vecdatastradiaec56.RData")
load("vecdatastradiaec57.RData")
load("vecdatastradiaec58.RData")
load("vecdatastradiaec59.RData")
load("vecdatastradiaec60.RData")
load("vecdatastradiaec61.RData")
load("vecdatastradiaec62.RData")
load("vecdatastradiaec63.RData")
load("vecdatastradiaec64.RData")
load("vecdatastradiaec65.RData")
load("vecdatastradiaec66.RData")
load("vecdatastradiaec67.RData")
load("vecdatastradiaec68.RData")
load("vecdatastradiaec69.RData")
load("vecdatastradiaec70.RData")


#join vec data

joinvecstradiaec <- mapply(vecdatastradiaec43,vecdatastradiaec44,vecdatastradiaec45,vecdatastradiaec46,vecdatastradiaec47,vecdatastradiaec48,vecdatastradiaec49,vecdatastradiaec50,vecdatastradiaec51,vecdatastradiaec52,vecdatastradiaec53,vecdatastradiaec54,vecdatastradiaec55,vecdatastradiaec56,vecdatastradiaec57,vecdatastradiaec58,vecdatastradiaec59,vecdatastradiaec60,vecdatastradiaec61,vecdatastradiaec62,vecdatastradiaec63,vecdatastradiaec64,vecdatastradiaec65,vecdatastradiaec66,vecdatastradiaec67,vecdatastradiaec68,vecdatastradiaec69,vecdatastradiaec70, FUN=list, SIMPLIFY=FALSE)

#split vectors to build df 

xdate <- joinvecstradiaec[1]
yads <- joinvecstradiaec[3]


#count number of Duplicates rows 

duplicatestradiaec43 <- sapply(vecdatastradiaec43[2], nrow)
duplicatestradiaec44 <- sapply(vecdatastradiaec44[2], nrow)
duplicatestradiaec45 <- sapply(vecdatastradiaec45[2], nrow)
duplicatestradiaec46 <- sapply(vecdatastradiaec46[2], nrow)
duplicatestradiaec47 <- sapply(vecdatastradiaec47[2], nrow)
duplicatestradiaec48 <- sapply(vecdatastradiaec48[2], nrow)
duplicatestradiaec49 <- sapply(vecdatastradiaec49[2], nrow)
duplicatestradiaec50 <- sapply(vecdatastradiaec50[2], nrow)
duplicatestradiaec51 <- sapply(vecdatastradiaec51[2], nrow)
duplicatestradiaec52 <- sapply(vecdatastradiaec52[2], nrow)
duplicatestradiaec53 <- sapply(vecdatastradiaec53[2], nrow)
duplicatestradiaec54 <- sapply(vecdatastradiaec54[2], nrow)
duplicatestradiaec55 <- sapply(vecdatastradiaec55[2], nrow)
duplicatestradiaec56 <- sapply(vecdatastradiaec56[2], nrow)
duplicatestradiaec57 <- sapply(vecdatastradiaec57[2], nrow)
duplicatestradiaec58 <- sapply(vecdatastradiaec58[2], nrow)
duplicatestradiaec59 <- sapply(vecdatastradiaec59[2], nrow)
duplicatestradiaec60 <- sapply(vecdatastradiaec60[2], nrow)
duplicatestradiaec61 <- sapply(vecdatastradiaec61[2], nrow)
duplicatestradiaec62 <- sapply(vecdatastradiaec62[2], nrow)
duplicatestradiaec63 <- sapply(vecdatastradiaec63[2], nrow)
duplicatestradiaec64 <- sapply(vecdatastradiaec64[2], nrow)
duplicatestradiaec65 <- sapply(vecdatastradiaec65[2], nrow)
duplicatestradiaec66 <- sapply(vecdatastradiaec66[2], nrow)
duplicatestradiaec67 <- sapply(vecdatastradiaec67[2], nrow)
duplicatestradiaec68 <- sapply(vecdatastradiaec68[2], nrow)
duplicatestradiaec69 <- sapply(vecdatastradiaec69[2], nrow)
duplicatestradiaec70 <- sapply(vecdatastradiaec70[2], nrow)


#create data frame considering duplicates 

dfduplicatestradiaec <- data.frame(c(duplicatestradiaec45,duplicatestradiaec44,duplicatestradiaec45,duplicatestradiaec46,duplicatestradiaec47,duplicatestradiaec48,duplicatestradiaec49,duplicatestradiaec50,duplicatestradiaec51,duplicatestradiaec52,duplicatestradiaec53,duplicatestradiaec54,duplicatestradiaec55,duplicatestradiaec56,duplicatestradiaec57,duplicatestradiaec58,duplicatestradiaec59,duplicatestradiaec60,duplicatestradiaec61,duplicatestradiaec62,duplicatestradiaec63,duplicatestradiaec64,duplicatestradiaec65,duplicatestradiaec66,duplicatestradiaec67,duplicatestradiaec68,duplicatestradiaec69,duplicatestradiaec70))



colnames(dfduplicatestradiaec) <- ("Duplicates Ads")

#create final data frame 

dfstradiaec <- data.frame(unlist(xdate),unlist(yads),dfduplicatestradiaec)

dfstradiaec$unlist.xdate. <- as.Date(dfstradiaec$unlist.xdate.,origin="1970-01-01")

colnames(dfstradiaec) <- c("Date","Ads","Duplicates")


stradiaec0107 <- data.frame(Date = as.Date("2017-07-01"),"Ads" = as.numeric(1512), "Duplicates" = as.numeric(11))
stradiaec0307 <- data.frame(Date = as.Date("2017-07-03"),"Ads" = as.numeric(1522), "Duplicates" = as.numeric(11))
stradiaec0807 <- data.frame(Date = as.Date("2017-07-08"),"Ads" = as.numeric(1084), "Duplicates" = as.numeric(8))


dfstradiaec <- rbind(dfstradiaec,stradiaec0107,stradiaec0307,stradiaec0807)

dfstradiaec <- dfstradiaec[order(as.Date(dfstradiaec$Date, format="%d/%m/%Y")),]

dfstradiaec$"perduplicates" <- percent(round(dfstradiaec$Duplicates/dfstradiaec$Ads,3))

rownames(dfstradiaec) <- NULL

save(dfstradiaec,file="dfstradiaec.RData")
load("dfstradiaec.RData")

#plot stradia

ggplot(dfstradiaec, aes(Date)) + 
geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
coord_cartesian(ylim = c(0, 2300)) + 
geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
geom_text(aes(y= Ads,label = Ads, vjust=2))

