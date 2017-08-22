#Objective: to analyse the merge impact of Stradia new code implementation
# we will start with Stradia India
# we will analyse gross ads, replies, visits and ad page impressions before and after the change (merge was done between 3-4 July 2017)
# period of analysis: 2017-04-02' to ' 2017-08-05'

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
# 2% compared with the 4 weeks prior to the merge. 
grossCmp[3,2]/grossCmp[1,2] -1



#NET ADS ------------------------------------ noneed for India as it's all net (no moderation)
# net<- ads%>%
#   filter(ad_counted %in% c(1))%>% #only 1
#   mutate(day=as.Date(dia))%>%
#   group_by(day)%>%
#   summarise(listings=sum(listings))%>%
#   mutate(period=ifelse(test$day<as.Date("2017-07-02"),"before","after"))
# 
# ggplot(data=net, aes(x=day, y=listings)) + geom_bar(stat="identity",fill="#BEC100") +
#       geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
#       theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
#       #scale_x_date(date_breaks  ="1 day") +
#       labs(title = "Weekly Gross Listings - Stradia IN")


 



# VISITS ---------------------------------------------------------------------


#Connection to Stradia IN web analytics data -----
#AT Internet until migration 3 July 
#Mixpanel after migration from 4 July
#data extracted as .csv

#read each AT file, add device column and bind them into "at_visits" data frame
vec<-c("at_visits_desktop.csv","at_visits_desktop_not_desktop.csv","at_visits_android.csv","at_visits_ios.csv")
dev<-c("desktop","rwd","android","ios")
at_visits <- data.frame()
for (i in seq_along(vec) ) {
    read_file<- read.csv(paste0("./data/", vec[i]),sep = ";") %>%
                  mutate(device=dev[i])
    colnames(read_file)<- c("dia","visits","device")
    at_visits <- rbind(at_visits, read_file)
}

#read each MIXPANEL file, add device column and bind them into "mp_visits" data frame
vec2<-c("mixpanel_sessions_desktop.csv","mixpanel_sessions_rwd.csv","mixpanel_sessions_android.csv","mixpanel_sessions_ios.csv")
mp_visits <- data.frame()
for (i in seq_along(vec2) ) {
    read_file<- read.csv(paste0("./data/", vec2[i]),sep = ";") %>%
                  mutate(device=dev[i]) %>% 
                  select(-value.pv)
    colnames(read_file)<- c("dia","visits","device")
    mp_visits <- rbind(mp_visits, read_file)
}

#rbind before and after periods
visits<- rbind(at_visits, mp_visits)
visits<- mutate(visits, dia=as.Date(dia,format = "%d/%m/%Y"))


#Analyse visits-----

visitsWeekly<- visits%>%
                mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                    ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#ECC61F") +
                    geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                    theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                    scale_x_date(date_breaks  ="1 week") +
                    labs(title = "Weekly Visits - Stradia IN", x = "weeks")

visitsWeeklyDevice<- visits%>%
                      mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                        ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#ECC61F") +
                        geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                        theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                        scale_x_date(date_breaks  ="1 week") +
                        labs(title = "Weekly Visits by Device - Stradia IN", x = "weeks") +
                        facet_wrap(~ device,nrow = 1)

# # align axis and build final graph
# grid.arrange(visitsWeekly, visitsWeeklyDevice, nrow=2, ncol=1) 
plot(visitsWeekly)
plot(visitsWeeklyDevice)

#compare 4 weeks before the merge vs 4 weeks after
#and calculate visits per week for the two periods 

##total
visitsCmp<- visits%>%
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(visits=sum(visits)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_visits_per_week=mean(visits))

# In the 4 weeks that followed the merge, the average visits per week dropped 
# by 24% compared with the 4 weeks prior to the merge. 
visitsCmp[3,2]/visitsCmp[1,2] -1

##desktop
visitsCmpDkt<- visits%>%
  filter(device =="desktop") %>% #filter only desktop
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(visits=sum(visits)) %>%  # it's grouped by week and device
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_visits_per_week=mean(visits))

# desktop was -58%
visitsCmpDkt[3,2]/visitsCmpDkt[1,2] -1

##rwd
visitsCmpRwd<- visits%>%
  filter(device =="rwd") %>%  #filter rwd
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(visits=sum(visits)) %>%  # it's grouped by week and device
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_visits_per_week=mean(visits))

# rwd was +30%
visitsCmpRwd[3,2]/visitsCmpRwd[1,2] -1

##ios
visitsCmpIos<- visits%>%
  filter(device =="ios") %>%  #filter ios
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(visits=sum(visits)) %>%  # it's grouped by week and device
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_visits_per_week=mean(visits))

# ios was +50%
visitsCmpIos[3,2]/visitsCmpIos[1,2] -1



# AD PAGE VIEWS --------------------------------------------------------

#Connection to Stradia IN web analytics data -----
#AT Internet until migration 3 July 
#Mixpanel after migration from 4 July
#data extracted as .csv

#read each AT file
vec3<-c("at_adpage_desktop.csv","at_adpage_desktop_not_desktop.csv","at_adpage_android.csv","at_adpage_ios.csv")
dev<-c("desktop","rwd","android","ios")
at_adpage <- data.frame()
for (i in seq_along(vec3) ) {
          read_file<- read.csv(paste0("./data/", vec3[i]),sep = ";") %>%
                          mutate(device=dev[i]) %>%
                          select(-Pages)
                          colnames(read_file)<- c("dia","loads","device")
            at_adpage <- rbind(at_adpage, read_file)
}

#read MP file
mp_adpage<- read.csv("data/ad_page_mixpanel.csv",sep = ",")

#Convert from wide to long format like AT file
mp_adpage_l<-gather(mp_adpage,device, loads, rwd:desktop, factor_key = T )
names(mp_adpage_l)<- c("dia","device","loads")

#rbind before and after periods # #
at_adpage$dia<- as.Date(at_adpage$dia,format = "%d/%m/%Y")
adpage<- rbind(at_adpage, mp_adpage_l)

#Analyse ad page views -----

#Plot!
adpageWeekly<- adpage%>%
                mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                  ggplot( aes(x=as.Date(week), y=loads)) + geom_bar(stat="identity",fill="#ABEBC6") +
                  geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                  theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                  scale_x_date(date_breaks  ="1 week") +
                  labs(title = "Weekly Ad Page Views - Stradia IN", x = "weeks")

adpageWeeklyDevice<- adpage%>%
                      mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                        ggplot( aes(x=as.Date(week), y=loads)) + geom_bar(stat="identity",fill="#ABEBC6") +
                        geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                        theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                        scale_x_date(date_breaks  ="1 week") +
                        labs(title = "Weekly Ad Page Views by Device - Stradia IN", x = "weeks") +
                        facet_wrap(~device, nrow = 1)

# # align axis and build final graph
# grid.arrange(adpageWeekly, adpageWeeklyDevice, nrow=2, ncol=1) 

plot(adpageWeekly)
plot(adpageWeeklyDevice)

#join visits with ad page views to calculate adpage_per_visit later
join_df<- left_join(visits,adpage, by=c("dia","device"))


#compare 4 weeks before the merge vs 4 weeks after
#and calculate adpage per week for the two periods 

##total  
adpageCmp<- join_df%>%
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(loads=sum(loads)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_loads_per_week=mean(loads))

# In the 4 weeks that followed the merge, the average loads per week increased 
# by 16% compared with the 4 weeks prior to the merge. 
adpageCmp[3,2]/adpageCmp[1,2] -1

##total excluding android which is not working  
adpageCmp<- join_df%>%
  filter(device !="android") %>%  
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(loads=sum(loads)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_loads_per_week=mean(loads))

# In the 4 weeks that followed the merge, the average loads per week increased 
# by 16% compared with the 4 weeks prior to the merge. 
adpageCmp[3,2]/adpageCmp[1,2] -1

##desktop
adpageCmpDkt<- join_df%>%
  filter(device =="desktop") %>%  #filter desktop
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(loads=sum(loads)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_loads_per_week=mean(loads))

# desktop +50%
adpageCmpDkt[3,2]/adpageCmpDkt[1,2] -1

##rwd
adpageCmpRwd<- join_df%>%
  filter(device =="rwd") %>%  #filter rwd
  mutate(week=cut(dia,breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(loads=sum(loads)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_loads_per_week=mean(loads))

# rwd +256%
adpageCmpRwd[3,2]/adpageCmpRwd[1,2] -1

##now calculate and analyse ad_page_per_visit
##even if we already know that should have increased overall
##since visits did not increased that much at least for desktops and rwd
join_df$adpage_per_visit<- join_df$loads/join_df$visits

adpage_per_visit_WeeklyDevice<- join_df%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%       
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate,device) %>%
  summarize(loads=sum(loads), visits=sum(visits)) %>%
  mutate(adpage_per_visit=loads/visits) %>%
  filter(device !="android")


#adpage_per_visit for rwd and desktop almost doubled after the merge.
ggplot(data=adpage_per_visit_WeeklyDevice, aes(x=weekDate, y=adpage_per_visit)) + 
    geom_bar(stat="identity",fill="#328366") +
    geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
    scale_x_date(date_breaks  ="1 week") +
    labs(title = "Weekly Ad Page Views per Visit by Device - Stradia IN", x = "weeks") +
      facet_wrap(~device, nrow = 1)


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

# # align axis and build final graph --
# grid.arrange(ansWeekly, ansWeeklySource, nrow=2, ncol=1)
plot(ansWeekly)
plot(ansWeeklySource)

#compare 4 weeks before the merge vs 4 weeks after

##total  
ansCmp<- replies_answers%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(answers=sum(answers)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_answers_per_week=mean(answers))

# In the 4 weeks that followed the merge, the average weekly answers increased 
# by 192% compared with the 4 weeks prior to the merge. 
ansCmp[3,2]/ansCmp[1,2] -1


##rwd
ansCmpRwd<- replies_answers%>%
  filter(device =="rwd") %>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(answers=sum(answers)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_answers_per_week=mean(answers))

# rwd +437% 
ansCmpRwd[3,2]/ansCmpRwd[1,2] -1


##desktop
ansCmpDsk<- replies_answers%>%
  filter(device =="desktop") %>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(answers=sum(answers)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_answers_per_week=mean(answers))

# desktop + 64%
ansCmpDsk[3,2]/ansCmpDsk[1,2] -1



# REPLIES SHOW PHONE NUMBER -----------------------------------------------
# will only do it for desktop and responsive
# because in the apps there is no "showing phone" event tracked, only clicks to call butoon

#Connection to Stradia IN web analytics data -----
#AT Internet until migration 3 July 
#Mixpanel after migration from 4 July
#data extracted as .csv

#read AT files
at_show_phone_dkt<- read.csv("./data/at_showing_phone_desktop.csv",sep = ";") %>%
                    mutate(device="desktop")
at_show_phone_rwd<- read.csv("./data/at_showing_phone_desktop_not_desktop.csv",sep = ";") %>%
                    mutate(device="rwd")
at_show_phone<- rbind(at_show_phone_dkt,at_show_phone_rwd) %>%
                select(-Action.type) 
names(at_show_phone)<- c("dia","show_phone","device")

#read MP file
mp_show_phone<- read.csv("./data/mixpanel_showing_phone.csv",sep = ",")
names(mp_show_phone)<-c("dia","rwd","ios","android","desktop")

#Convert from wide to long format like AT file
mp_show_phone_l<-gather(mp_show_phone,device, show_phone, rwd:desktop, factor_key = T )

#rbind before and after periods
at_show_phone$dia<- as.Date(at_show_phone$dia,format = "%d/%m/%Y")
show_phone<- rbind(at_show_phone, mp_show_phone_l) %>%
                filter(device %in% c("desktop","rwd"))  #remove apps since I dont need to compare it  

# Analyse show phone -----

phnWeekly<- show_phone%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
  ggplot( aes(x=as.Date(week), y=show_phone)) + geom_bar(stat="identity",fill="#CAB03D") +
  geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5),plot.subtitle = element_text(hjust = 0.5)) +
  scale_x_date(date_breaks  ="1 week") +
  labs(title = "Weekly Show Phone - Stradia IN", x = "weeks", subtitle="only desktop+rwd")

phnWeeklySource<- show_phone%>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
  ggplot( aes(x=as.Date(week), y=show_phone)) + geom_bar(stat="identity",fill="#CAB03D") +
  geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
  scale_x_date(date_breaks  ="1 week") +
  labs(title = "Weekly Show Phone by Device - Stradia IN", x = "weeks") +
  facet_wrap(~ device, nrow=1)

plot(phnWeekly)
plot(phnWeeklySource)

##rwd
phnCmpRwd<- show_phone%>%
  filter(device =="rwd") %>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(show_phone=sum(show_phone)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_show_phone_per_week=mean(show_phone))

# rwd
phnCmpRwd[3,2]/phnCmpRwd[1,2] -1

##desktop
phnCmpDsk<- show_phone%>%
  filter(device =="desktop") %>%
  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%
  mutate(weekDate=as.Date(as.character(week))) %>%
  group_by(weekDate) %>%
  summarize(show_phone=sum(show_phone)) %>%  # it's grouped by week
  filter(weekDate >= "2017-06-04") %>%   # I filter until 4 weeks before
  mutate(WeekN = c(rep("t1",4),"t2",rep("t3",4))) %>%
  group_by(WeekN) %>%
  summarize(avg_show_phone_per_week=mean(show_phone))

# desktop + 255%
phnCmpDsk[3,2]/phnCmpDsk[1,2] -1
