
library(RGA)
library(dplyr)
library("RMySQL")
library(googleVis)
library(stringr)
library(readxl)
library("scales")
library("lubridate")


conn_imo <-  dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3309)

#load 

imovastotalv <- "SELECT YEARWEEK(last_status_date) as Week, COUNT(ps.id) as 'Total VAS' FROM imovirtualpt.payment_session ps INNER JOIN imovirtualpt.payment_basket pb ON ps.id = pb.session_id INNER JOIN imovirtualpt.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN imovirtualpt.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
imovastotalv <- dbGetQuery(conn_imo,imovastotalv)

imovasv <- "SELECT YEARWEEK(last_status_date) as Week,COUNT(CASE WHEN code LIKE '%ad_homepage%' THEN ps.id ELSE NULL END) AS 'ad_homepage',COUNT(CASE WHEN code LIKE '%highlight%' THEN ps.id ELSE NULL END) AS 'highlight',COUNT(CASE WHEN code LIKE '%header%' THEN ps.id ELSE NULL END) AS 'header',COUNT(CASE WHEN code LIKE '%pushup%' THEN ps.id ELSE NULL END) AS 'pushup',COUNT(CASE WHEN code LIKE '%mirror%' THEN ps.id ELSE NULL END) AS 'mirror' FROM imovirtualpt.payment_session ps INNER JOIN imovirtualpt.payment_basket pb ON ps.id = pb.session_id INNER JOIN imovirtualpt.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN imovirtualpt.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
imovasv <- dbGetQuery(conn_imo,imovasv)

imorevtotal <- "SELECT YEARWEEK(last_status_date) as Week, COUNT(ps.id) as 'Total', ROUND(SUM(ABS(price))/1.23,0) AS 'Total Revenue' FROM imovirtualpt.payment_session ps INNER JOIN imovirtualpt.payment_basket pb ON ps.id = pb.session_id INNER JOIN imovirtualpt.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN imovirtualpt.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
imorevtotal <- dbGetQuery(conn_imo,imorevtotal)

imorevas <- "SELECT YEARWEEK(last_status_date) as Week,ROUND(SUM(ABS(CASE WHEN code LIKE '%ad_homepage%' THEN price/1.23 ELSE NULL END))) AS 'ad_homepage',ROUND(SUM(ABS(CASE WHEN code LIKE '%highlight%' THEN price/1.23 ELSE NULL END))) AS 'highlight',ROUND(SUM(ABS(CASE WHEN code LIKE '%header%' THEN price/1.23 ELSE NULL END))) AS 'header',ROUND(SUM(ABS(CASE WHEN code LIKE '%pushup%' THEN price/1.23 ELSE NULL END))) AS 'pushup',ROUND(SUM(ABS(CASE WHEN code LIKE '%mirror%' THEN price/1.23 ELSE NULL END))) AS 'mirror' FROM imovirtualpt.payment_session ps  INNER JOIN imovirtualpt.payment_basket pb ON ps.id = pb.session_id INNER JOIN imovirtualpt.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN imovirtualpt.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
imorevas <- dbGetQuery(conn_imo,imorevas)


dbDisconnect(conn_imo)


######--------transform-------------------------------------------###########


imorevas <- imorevas[-10,]
imorevtotal <- imorevtotal[-10,]
imovastotalv <- imovastotalv[-10,]
imovasv <- imovasv[-10,]

save(imorevas,file="imorevas.RData")
save(imorevtotal,file="imorevtotal.RData")
save(imovasv,file="imovasv.RData")
save(imovastotalv,file="imototalv.RData")

###############VAS user and Volume##############

# otodomvasvoltop <- otodomvasv[c(1,3,5,6)]
# otodomvasusertop <- otodomvasuser[c(1,3,5,6)]
# colnames(otodomvasvoltop) <- c('Week','Vol Export_olx','Vol Pushup','Vol Topads')
# colnames(otodomvasusertop) <- c('Week','Export_olx Users','Pushup Users','Topads Users')
# 
# otodomvastop <- cbind(otodomvasusertop,otodomvasvoltop)
# otodomvastop <- otodomvastop[c(1,2,3,4,6,7,8)]


#######Total VAS and User###########

#otodomvasusertotal <- t(otodomvasusertotal)
#otodomvasusertotal <- as.data.frame(otodomvasusertotal)
#colnames(otodomvasusertotal) <- c('Users')

#otodomvasvtotal <- t(otodomvasvtotal)
#otodomvasvtotal <- as.data.frame(otodomvasvtotal)
#colnames(otodomvasvtotal) <- c('Purchases')

#otodomvastotal <- cbind(otodomvasusertotal,otodomvasvtotal)
#otodomvastotal$Vas <- c('ad_homepage','export_olx','header','pushup','topads','transactional_maps')


######## % TM by B2B VAS############

# otodomtranst <- cbind(otodomtranst,otodomvast)
# 
# otodomtranst$'%Purchases' <- percent(otodomtranst$Transactional/otodomtranst$Total)
# otodomtranst$'%Revenue' <- percent(otodomtranst$'Trans Revenue'/otodomtranst$'Total Revenue')
# 
# 
# otodomtranst$'%Purchases' <- as.numeric(sub("%","",otodomtranst$'%Purchases'))
# otodomtranst$'%Revenue' <- as.numeric(sub("%","",otodomtranst$'%Revenue'))
# 
# 
# otodomtranst <- otodomtranst[c(1,2,3,5,6,7,8)]
# 
# save(otodomtranst,file='otodomtranst.RData')


####### Total Revenue #############

# 
# otodomrevtotal <- t(otodomrevenuetotal)
# otodomrevenuetotal <- as.data.frame(otodomrevenuetotal)
# colnames(otodomrevenuetotal) <- c('Revenue')
# otodomrevenuetotal$Vas <- c('ad_homepage','export_olx','header','pushup','topads','transactional_maps')
# 
# save(otodomrevenuetotal,file="otodomrevenuetotal.RData")



require(googleVis)

plot(gvisLineChart(imovasv,
                   xvar = 'Week', yvar = c('ad_homepage','highlight','header','pushup','mirror'), options = list(
                     legend = 'yes',
                     title="VAS by Week",
                     #vAxes="[{viewWindowMode:'explicit',
                     #viewWindow:{min:0, max:370000}}]",
                     width=1200, height=600,
                     vAxes="[{title:'Purchases',
                     format:'##'}]",
                     hAxes="[{title:'Week',
                     textPosition: 'out'}]",
                     backgroundColor = "{fill:'transparent'}"))
)

plot(gvisAreaChart(imovastotalv,
                   xvar = 'Week', yvar = c('Total VAS'), options = list(
                     legend = 'yes',
                     title="Total VAS by Week",
                     #vAxes="[{viewWindowMode:'explicit',
                     #viewWindow:{min:0, max:370000}}]",
                     width=1200, height=600,
                     vAxes="[{title:'Purchases',
                     format:'##'}]",
                     hAxes="[{title:'Week',
                     textPosition: 'out'}]",
                     colors = "['#00CCCC']",
                     backgroundColor = "{fill:'transparent'}"))
)


plot(gvisComboChart(imovastop, 
                    xvar = 'Week', yvar = c('Export_olx Users','Pushup Users','Topads Users'), options = list(
                      legend = 'yes', 
                      title="Transactional Maps Revenue by Week",
                      width=1200, height=600, 
                      vAxes="[{title:'Revenue (zl)',
                      format:'##'}]",
                      hAxes="[{title:'Week',
                      textPosition: 'out'}]", 
                      backgroundColor = "{fill:'transparent'}"))
)       

plot(gvisLineChart(imorevas,
                   xvar = 'Week', yvar = c('ad_homepage','highlight','header','pushup','mirror'), options = list(
                     legend = 'yes',
                     title="Revenue by Week",
                     #vAxes="[{viewWindowMode:'explicit',
                     #viewWindow:{min:0, max:370000}}]",
                     width=1200, height=600,
                     vAxes="[{title:'Purchases',
                     format:'##'}]",
                     hAxes="[{title:'Week',
                     textPosition: 'out'}]",
                     backgroundColor = "{fill:'transparent'}"))
)

plot(gvisAreaChart(imorevtotal,
                   xvar = 'Week', yvar = c('Total Revenue'), options = list(
                     legend = 'yes',
                     title="Total Revenue by Week",
                     #vAxes="[{viewWindowMode:'explicit',
                     #viewWindow:{min:0, max:370000}}]",
                     width=1200, height=600,
                     vAxes="[{title:'Purchases',
                     format:'##'}]",
                     hAxes="[{title:'Week',
                     textPosition: 'out'}]",
                     backgroundColor = "{fill:'transparent'}"))
)



