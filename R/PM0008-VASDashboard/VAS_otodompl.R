
library(RGA)
library(dplyr)
library("RMySQL")
library(googleVis)
library(stringr)
library(readxl)
library("scales")
library("lubridate")


conn_otodom <-  dbConnect(RMySQL::MySQL(), username = "bi_team_pt", password = "bi5Zv3TB", host = "192.168.1.5", port = 3321)

#load 

otodomvastotalv <- "SELECT YEARWEEK(last_status_date) as Week, COUNT(ps.id) as 'Total VAS' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
otodomvastotalv <- dbGetQuery(conn_otodom,otodomvastotalv)

otodomvasv <- "SELECT YEARWEEK(last_status_date) as Week,COUNT(CASE WHEN code LIKE '%ad_homepage%' THEN ps.id ELSE NULL END) AS 'ad_homepage',COUNT(CASE WHEN code LIKE '%export_olx%' THEN ps.id ELSE NULL END) AS 'export_olx',COUNT(CASE WHEN code LIKE '%header%' THEN ps.id ELSE NULL END) AS 'header',COUNT(CASE WHEN code LIKE '%pushup%' THEN ps.id ELSE NULL END) AS 'pushup',COUNT(CASE WHEN code LIKE '%topads%' THEN ps.id ELSE NULL END) AS 'topads',COUNT(CASE WHEN code LIKE '%transactional%' THEN ps.id ELSE NULL END) AS 'transactional_maps' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
otodomvasv <- dbGetQuery(conn_otodom,otodomvasv)

#otodomvasuser <- "SELECT WEEK(last_status_date)as Week,COUNT(DISTINCT CASE WHEN code LIKE '%ad_homepage%' THEN pb.user_id ELSE NULL END) AS 'ad_homepage',COUNT(DISTINCT CASE WHEN code LIKE '%export_olx%' THEN pb.user_id ELSE NULL END) AS 'export_olx',COUNT(DISTINCT CASE WHEN code LIKE '%header%' THEN pb.user_id ELSE NULL END) AS 'header',COUNT(DISTINCT CASE WHEN code LIKE '%pushup%' THEN pb.user_id ELSE NULL END) AS 'pushup',COUNT(DISTINCT CASE WHEN code LIKE '%topads%' THEN pb.user_id ELSE NULL END) AS 'topads',COUNT(DISTINCT CASE WHEN code LIKE '%transactional%' THEN pb.user_id ELSE NULL END) AS 'transactional_maps' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-01-01' AND '2018-02-24 23:59:59' AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
#otodomvasuser <- dbGetQuery(conn_otodom,otodomvasuser)

#otodomvasvtotal <- "SELECT COUNT(CASE WHEN code LIKE '%ad_homepage%' THEN ps.id ELSE NULL END) AS 'ad_homepage',COUNT(CASE WHEN code LIKE '%export_olx%' THEN ps.id ELSE NULL END) AS 'export_olx',COUNT(CASE WHEN code LIKE '%header%' THEN ps.id ELSE NULL END) AS 'header',COUNT(CASE WHEN code LIKE '%pushup%' THEN ps.id ELSE NULL END) AS 'pushup',COUNT(CASE WHEN code LIKE '%topads%' THEN ps.id ELSE NULL END) AS 'topads',COUNT(CASE WHEN code LIKE '%transactional%' THEN ps.id ELSE NULL END) AS 'transactional_maps' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-01-01' AND '2018-02-24 23:59:59' AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51);"
#otodomvasvtotal <- dbGetQuery(conn_otodom,otodomvasvtotal)

#otodomvasusertotal <- "SELECT COUNT(DISTINCT CASE WHEN code LIKE '%ad_homepage%' THEN pb.user_id ELSE NULL END) AS 'ad_homepage',COUNT(DISTINCT CASE WHEN code LIKE '%export_olx%' THEN pb.user_id ELSE NULL END) AS 'export_olx',COUNT(DISTINCT CASE WHEN code LIKE '%header%' THEN pb.user_id ELSE NULL END) AS 'header',COUNT(DISTINCT CASE WHEN code LIKE '%pushup%' THEN pb.user_id ELSE NULL END) AS 'pushup',COUNT(DISTINCT CASE WHEN code LIKE '%topads%' THEN pb.user_id ELSE NULL END) AS 'topads',COUNT(DISTINCT CASE WHEN code LIKE '%transactional%' THEN pb.user_id ELSE NULL END) AS 'transactional_maps' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-01-01' AND '2018-02-24 23:59:59' AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51);"
#otodomvasusertotal <- dbGetQuery(conn_otodom,otodomvasusertotal)


otodomrevtotal <- "SELECT YEARWEEK(last_status_date) as Week, COUNT(ps.id) as 'Total', ROUND(SUM(ABS(price))/1.23,0) AS 'Total Revenue' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
otodomrevtotal <- dbGetQuery(conn_otodom,otodomrevtotal)

otodomrevas <- "SELECT YEARWEEK(last_status_date) as Week, ROUND(SUM(ABS(CASE WHEN code LIKE '%ad_homepage%' THEN price/1.23 ELSE NULL END))) AS 'ad_homepage',ROUND(SUM(ABS(CASE WHEN code LIKE '%export_olx%' THEN price/1.23 ELSE NULL END))) AS 'export_olx',ROUND(SUM(ABS(CASE WHEN code LIKE '%header%' THEN price/1.23 ELSE NULL END))) AS 'header',ROUND(SUM(ABS(CASE WHEN code LIKE '%pushup%' THEN price/1.23 ELSE NULL END))) AS 'pushup',ROUND(SUM(ABS(CASE WHEN code LIKE '%topads%' THEN price/1.23 ELSE NULL END))) AS 'topads',ROUND(SUM(ABS(CASE WHEN code LIKE '%transactional%' THEN price/1.23 ELSE NULL END))) AS 'transactional_maps' FROM otodompl.payment_session ps INNER JOIN otodompl.payment_basket pb ON ps.id = pb.session_id INNER JOIN otodompl.paidads_indexes pi ON pi.id = pb.index_id INNER JOIN otodompl.users u ON pb.user_id = u.id WHERE last_status_date BETWEEN '2018-02-01' AND NOW() AND status = 'finished' AND provider NOT IN ('account','admin','protocol','nocharge') AND is_business = 1 AND pi.id NOT IN (73,51) GROUP BY 1;"
otodomrevas <- dbGetQuery(conn_otodom,otodomrevas)


dbDisconnect(conn_otodom)


######--------transform-------------------------------------------###########


save(otodomrevas,file="otodomrevas.RData")
save(otodomrevtotal,file="otodomrevtotal.RData")
save(otodomvasv,file="otodomvasv.RData")
save(otodomvastotalv,file="otodomtotalv.RData")

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

plot(gvisLineChart(otodomvasv,
                   xvar = 'Week', yvar = c('ad_homepage','export_olx','header','pushup','topads','transactional_maps'), options = list(
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

plot(gvisAreaChart(otodomvastotalv,
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


plot(gvisComboChart(otodomvastop, 
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

plot(gvisLineChart(otodomrevas,
                   xvar = 'Week', yvar = c('ad_homepage','export_olx','header','pushup','topads','transactional_maps'), options = list(
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

plot(gvisAreaChart(otodomrevtotal,
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



