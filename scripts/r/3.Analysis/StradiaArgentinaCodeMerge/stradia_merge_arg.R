#STRADIA ARGENTINA
#Objective: to analyse the merge impact of Stradia new code implementation
# we will start with Stradia India
# we will analyse gross ads, replies, visits and ad page impressions before and after the change (merge was done between 3-4 July 2017)
# period of analysis: 2017-04-02' to ' 2017-08-19'

setwd("~/verticals-bi/scripts/r/3.Analysis/StradiaArgentinaCodeMerge")

#Load libraries
library("DBI")
library("RMySQL")
library("dplyr")
library("tidyr")
library("ggplot2")
library("gridExtra")

#Connection to Stradia AR DB --------------------------------------

load("~/r_scripts_miei/credentials.Rdata")

dbUsername <- cfStradiaLatamDbUser
dbPassword <- cfStradiaLatamDbPassword
dbHost <- cfStradiaLatamDbHost
dbPort <- cfStradiaLatamDbPort
dbName <- cfStradiaArdbName

conDB<- dbConnect(MySQL(), 
                  user=dbUsername, 
                  password=dbPassword,
                  host=dbHost, 
                  port= 3311,
                  dbname = dbName
)

# #list tables
# dbListTables(conDB)

#edit SQL queries
sql_string_ads<- "
SELECT
DATE(created_at_first) as dia,
net_ad_counted as ad_counted,
COUNT(DISTINCT id) as listings
FROM stradia_ar.ads
WHERE created_at_first>=  ' 2017-04-02' AND created_at_first<= ' 2017-08-19'
GROUP BY dia, ad_counted
;"

sql_string_replies_answers<- "
-- replies answers
SELECT
DATE(posted) as dia,
CASE 
	WHEN source= 'none' THEN 'desktop'
  WHEN source= 'apple' THEN 'ios'
  ELSE source
END AS device,
COUNT(DISTINCT id) as answers
FROM stradia_ar.answers
WHERE posted>=  ' 2017-04-02' AND posted<= ' 2017-08-19'
AND spam_status IN ('ok', 'probably_ok')
AND user_id = seller_id AND buyer_id = sender_id AND parent_id = 0
GROUP BY dia, device
;"

#extract data
ads <-dbGetQuery(conDB, sql_string_ads)
replies_answers <-dbGetQuery(conDB, sql_string_replies_answers)

#close connection
dbDisconnect(conDB)

#check data
head(ads)
#there is no difference between gross vs net, are all 1


#GROSS ADS -------------------------------------------------

#Analyse gross ads -----

gross<- ads%>%
            filter(ad_counted %in% c(0,1))%>%   # 0 or 1
            mutate(day=as.Date(dia))%>%
            group_by(day)%>%
            summarise(listings=sum(listings)) 

#daily chart -- noticed a drop beginning of July and inmediate peak later when ads where eventually exported.
grossDaily<-  ggplot(data=gross, aes(x=day, y=listings)) + geom_bar(stat="identity",fill="#BEC100") +
                geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
                #scale_x_date(date_breaks  ="1 day") +
                labs(title = "Daily Gross Listings - Stradia AR")

#weekly chart
grossWeekly<- gross%>%
              mutate(week=cut(day,breaks="week", start.on.monday=FALSE)) %>%        
                ggplot( aes(x=as.Date(week), y=listings)) + geom_bar(stat="identity",fill="#BEC100") +
                geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
                scale_x_date(date_breaks  ="1 week") +
                labs(title = "Weekly Gross Listings - Stradia AR", x = "weeks")
 
# align axis and build final graph
gb1 <- ggplot_build(grossDaily)
gb2 <- ggplot_build(grossWeekly)

n1 <- length(gb1$layout$panel_params[[1]]$y.labels)
n2 <- length(gb2$layout$panel_params[[1]]$y.labels)

gA <- ggplot_gtable(gb1)
gB <- ggplot_gtable(gb2)

plot(rbind(gA, gB))

# or simply,but not aligned: grid.arrange(g1, g2, nrow=2, ncol=1) 

#compare 4 weeks before the merge vs 4 weeks after
#and calculate average gross listings per week for the two periods 
grossCmp<- gross%>%
  mutate(week=cut(day,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(listings=sum(listings)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04" & weekDate <="2017-07-30") %>%   # I filter until 4 weeks before and 4 weeks after
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_listings_per_week=mean(listings))

# In the 4 weeks that followed the merge, the average gross listings per week increased by
# 147% compared with the 4 weeks prior to the merge. 
grossCmp[3,2]/grossCmp[1,2] -1


# VISITS ---------------------------------------------------------------------


#Connection to Stradia IN web analytics data -----
#GA until migration 3 July 
#Mixpanel after migration from 4 July
#data extracted as .csv

#GA extract data via API
library(RGA)
id_web <- "ga:123353820"
id_ios <- "ga:123306182"
id_and <- "ga:123298983"
start_date <- "2017-04-02"
end_date <- "2017-07-03"

#Load the file containing the Authorization to Access GA (ising the one from Pedro)
load("~/verticals-bi/R/PM0005-Storia ID Migration/tokenGA.RData")

#web profile
ga_visits_web <- data.frame(get_ga(profileId = id_web, start.date = start_date,
                                   end.date = end_date, metrics = c("ga:sessions"), 
                                   dimensions = c("ga:date","ga:deviceCategory"),  sort = NULL, filters = "",
                                   segment = NULL, samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))
#very little data before merge

#ios profile
ga_visits_ios <- data.frame(get_ga(profileId = id_ios, start.date = start_date,
                                   end.date = end_date, metrics = c("ga:sessions"), 
                                   dimensions = c("ga:date","ga:deviceCategory"),  sort = NULL, filters = "",
                                   segment = NULL, samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))
#almost no data before merge

#android profile
ga_visits_and <- data.frame(get_ga(profileId = id_and, start.date = start_date,
                                   end.date = end_date, metrics = c("ga:sessions"), 
                                   dimensions = c("ga:date","ga:deviceCategory"),  sort = NULL, filters = "",
                                   segment = NULL, samplingLevel = NULL, start.index = NULL,
                                   max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_tokenGA))
#almost no data before merge
ga_visits_web$
  
#combine ga datasets
ga_visits_web$deviceCategory[ga_visits_web$deviceCategory %in% c("mobile","tablet")]<-"rwd"
ga_visits_ios$deviceCategory <- "ios"
ga_visits_and$deviceCategory <- "android"
ga_visits <- rbind(ga_visits_web,ga_visits_ios,ga_visits_and)
names(ga_visits) <- c("dia","device","visits")


#read each MIXPANEL file, add device column and bind them into "mp_visits" data frame
setwd("~/verticals-bi/scripts/r/3.Analysis/StradiaArgentinaCodeMerge")
vec2<-c("mixpanel_sessions_desktop_ar.csv","mixpanel_sessions_rwd_ar.csv","mixpanel_sessions_android_ar.csv","mixpanel_sessions_ios_ar.csv")
dev<-c("desktop","rwd","android","ios")
mp_visits <- data.frame()
for (i in seq_along(vec2) ) {
  read_file<- read.csv(paste0("./data/", vec2[i]),sep = ";") %>%
                  mutate(device=dev[i]) %>%
                  select(-value.pv)
    colnames(read_file)<- c("dia","visits","device")
    mp_visits <- rbind(mp_visits, read_file)
}

#rbind before and after periods
ga_visits<- mutate(ga_visits, dia=as.Date(dia))
mp_visits<- mutate(mp_visits, dia=as.Date(dia,format = "%d/%m/%Y"))
visits<- rbind(ga_visits, mp_visits)
visits<- mutate(visits, dia=as.Date(dia,format = "%d/%m/%Y"))


#Analyse visits-----

visitsWeekly<- visits%>%
                mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
                    ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#F9E79F") +
                    geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                    theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                    scale_x_date(date_breaks  ="1 week") +
                    labs(title = "Weekly Visits - Stradia AR", x = "weeks")

visitsWeeklyDevice<- visits%>%
                      mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
                        ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#F9E79F") +
                        geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                        theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                        scale_x_date(date_breaks  ="1 week") +
                        labs(title = "Weekly Visits by Device - Stradia AR", x = "weeks") +
                        facet_wrap(~ device,nrow = 1)
# 
# # align axis and build final graph
# grid.arrange(visitsWeekly, visitsWeeklyDevice, nrow=2, ncol=1)
plot(visitsWeekly)
plot(visitsWeeklyDevice)


# 
# #compare 4 weeks before the merge vs 4 weeks after
# #and calculate visits per week for the two periods 
# 
# ##total
# visitsCmp<- visits%>%
#   mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
#   mutate(weekDate=as.Date(as.character(week))) %>%
#   group_by(weekDate) %>%
#   summarize(visits=sum(visits)) %>%  # it's grouped by week
#   filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
#   mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
#   group_by(WeekN) %>%
#   summarize(avg_visits_per_week=mean(visits))
# 
# # In the 4 weeks that followed the merge, the average visits per week dropped 
# # by 24% compared with the 4 weeks prior to the merge. 
# visitsCmp[3,2]/visitsCmp[1,2] -1
# 
# ##desktop
# visitsCmpDkt<- visits%>%
#   filter(device =="desktop") %>% #filter only desktop
#   mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
#   mutate(weekDate=as.Date(as.character(week))) %>%
#   group_by(weekDate) %>%
#   summarize(visits=sum(visits)) %>%  # it's grouped by week and device
#   filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
#   mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
#   group_by(WeekN) %>%
#   summarize(avg_visits_per_week=mean(visits))
# 
# # desktop was -58%
# visitsCmpDkt[3,2]/visitsCmpDkt[1,2] -1
# 
# ##rwd
# visitsCmpRwd<- visits%>%
#   filter(device =="rwd") %>%  #filter rwd
#   mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
#   mutate(weekDate=as.Date(as.character(week))) %>%
#   group_by(weekDate) %>%
#   summarize(visits=sum(visits)) %>%  # it's grouped by week and device
#   filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
#   mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
#   group_by(WeekN) %>%
#   summarize(avg_visits_per_week=mean(visits))
# 
# # rwd was +30%
# visitsCmpRwd[3,2]/visitsCmpRwd[1,2] -1
# 
# ##ios
# visitsCmpIos<- visits%>%
#   filter(device =="ios") %>%  #filter ios
#   mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
#   mutate(weekDate=as.Date(as.character(week))) %>%
#   group_by(weekDate) %>%
#   summarize(visits=sum(visits)) %>%  # it's grouped by week and device
#   filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
#   mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
#   group_by(WeekN) %>%
#   summarize(avg_visits_per_week=mean(visits))
# 
# # ios was +50%
# visitsCmpIos[3,2]/visitsCmpIos[1,2] -1
# 
# 
# 


# REPLIES ANSWERS -----------------------------------------------

#Analyse answers -----

ansWeekly<- replies_answers%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
  ggplot( aes(x=as.Date(week), y=answers)) + geom_bar(stat="identity",fill="#FEC100") +
  geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
  scale_x_date(date_breaks  ="1 week") +
  labs(title = "Weekly Answers - Stradia AR", x = "weeks")

ansWeeklySource<- replies_answers%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
  ggplot( aes(x=as.Date(week), y=answers)) + geom_bar(stat="identity",fill="#FEC100") +
  geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
  scale_x_date(date_breaks  ="1 week") +
  labs(title = "Weekly Answers by Device - Stradia AR", x = "weeks") +
  facet_wrap(~ device, nrow=1)

plot(ansWeekly)
plot(ansWeeklySource)

#compare 4 weeks before the merge vs 4 weeks after

##total  no data for the week 2017-07-18
ansCmp<- replies_answers%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(answers=sum(answers)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04"& weekDate <="2017-07-30") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",3),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_answers_per_week=mean(answers))

#from 2 to 48 per week  
ansCmp[3,2]/ansCmp[1,2] -1

# rwd have data only after the merge


