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

#Connection to Stradia IN DB --------------------------------------

load("~/r_scripts_miei/credentials.Rdata")

dbUsername <- cfStradiaInDbUser
dbPassword <- cfStradiaInDbPassword
dbHost <- cfStradiaInDbHost
dbPort <- cfStradiaInDbPort
dbName <- cfStradiaIndbName

conDB<- dbConnect(MySQL(), 
                  user=dbUsername, 
                  password=dbPassword,
                  host=dbHost, 
                  port= 3312,
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
FROM cars_in.ads
WHERE created_at_first>=  ' 2017-04-02' AND created_at_first<= ' 2017-08-05'
GROUP BY dia, ad_counted
;"

sql_string_replies<- "
-- replies
SELECT
DATE(posted) as dia,
source,
COUNT(DISTINCT id) as listings
FROM cars_in.answers
WHERE posted>=  ' 2017-04-02' AND posted<= ' 2017-08-05'
AND spam_status IN ('ok', 'probably_ok')
AND user_id = seller_id AND buyer_id = sender_id AND parent_id = 0
GROUP BY dia, source
;"

#extract data
ads <-dbGetQuery(conDB, sql_string_ads)
replies <-dbGetQuery(conDB, sql_string_replies)

#close connection
dbDisconnect(conDB)

#check data
head(ads)
#there is no difference between gross vs net, are all 1


#GROSS ADS -------------------------------------------------
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
                labs(title = "Daily Gross Listings - Stradia IN")

#weekly chart
grossWeekly<- gross%>%
              mutate(week=cut(day,breaks="week", start.on.monday=FALSE)) %>%        
                ggplot( aes(x=as.Date(week), y=listings)) + geom_bar(stat="identity",fill="#BEC100") +
                geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
                scale_x_date(date_breaks  ="1 week") +
                labs(title = "Weekly Gross Listings - Stradia IN", x = "weeks")
 
# align axis and build final graph -----------
gb1 <- ggplot_build(grossDaily)
gb2 <- ggplot_build(grossWeekly)

n1 <- length(gb1$layout$panel_params[[1]]$y.labels)
n2 <- length(gb2$layout$panel_params[[1]]$y.labels)

gA <- ggplot_gtable(gb1)
gB <- ggplot_gtable(gb2)

plot(rbind(gA, gB))

# or simply,but not aligned: grid.arrange(g1, g2, nrow=2, ncol=1) 


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


#REPLIES -------------------------------------
#will do it only weekly to remove seasonality
repliesWeekly<- replies%>%
                  mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                    ggplot( aes(x=as.Date(week), y=listings)) + geom_bar(stat="identity",fill="#FEC100") +
                    geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                    theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
                    scale_x_date(date_breaks  ="1 week") +
                    labs(title = "Weekly Replies - Stradia IN", x = "weeks")

repliesWeeklySource<- replies%>%
                        mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                          ggplot( aes(x=as.Date(week), y=listings)) + geom_bar(stat="identity",fill="#FEC100") +
                          geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                          theme(axis.text.x = element_text(angle = 90, hjust = 1),plot.title = element_text(hjust = 0.5)) +
                          scale_x_date(date_breaks  ="1 week") +
                          labs(title = "Weekly Replies by Source - Stradia IN", x = "weeks") +
                          facet_wrap(~ source)

# align axis and build final graph --
grid.arrange(repliesWeekly, repliesWeeklySource, nrow=2, ncol=1) 


#Connection to Stradia IN web analytics data -------------------------------------
#AT Internet until migration 3 July 
#Mixpanel after migration from 4 July
#data extracted as .csv

#read each AT file, add device column and bind them into "at_visits" data frame
vec<-c("at_visits_desktop.csv","at_visits_desktop_not_desktop.csv","at_visits_android.csv","at_visits_ios.csv")
dev<-c("desktop","rwd","android","ios")
at_visits <- data.frame()
for (i in seq_along(vec) ) {
    read_file<- read.csv(vec[i],sep = ";") %>%
                  mutate(device=dev[i])
    colnames(read_file)<- c("dia","visits","device")
    at_visits <- rbind(at_visits, read_file)
}

#read each MIXPANEL file, add device column and bind them into "mp_visits" data frame
vec2<-c("mixpanel_sessions_desktop.csv","mixpanel_sessions_rwd.csv","mixpanel_sessions_android.csv","mixpanel_sessions_ios.csv")
mp_visits <- data.frame()
for (i in seq_along(vec2) ) {
    read_file<- read.csv(vec2[i],sep = ";") %>%
                  mutate(device=dev[i]) %>% 
                  select(-value.pv)
    colnames(read_file)<- c("dia","visits","device")
    mp_visits <- rbind(mp_visits, read_file)
}

#rbind before and after periods
visits<- rbind(at_visits, mp_visits)
visits<- mutate(visits, dia=as.Date(dia,format = "%d/%m/%Y"))

#VISITS ------------------------------------------------------------

visitsWeekly<- visits%>%
                mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                    ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#c2c502") +
                    geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                    theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                    scale_x_date(date_breaks  ="1 week") +
                    labs(title = "Weekly Visits - Stradia IN", x = "weeks")

visitsWeeklyDevice<- visits%>%
                      mutate(week=cut(as.Date(dia),breaks="week", start.on.monday=FALSE)) %>%        
                        ggplot( aes(x=as.Date(week), y=visits)) + geom_bar(stat="identity",fill="#c2c502") +
                        geom_vline(xintercept = as.numeric(as.Date("2017-07-02")), linetype=4) +
                        theme(axis.text.x = element_text(angle = 90, hjust = 1), plot.title = element_text(hjust = 0.5)) +
                        scale_x_date(date_breaks  ="1 week") +
                        labs(title = "Weekly Visits by Device - Stradia IN", x = "weeks") +
                        facet_wrap(~ device)

# align axis and build final graph -----------
grid.arrange(visitsWeekly, visitsWeeklyDevice, nrow=2, ncol=1) 


#AD PAGE VIEWS --------------------------------------------------------

#read each AT file
vec3<-c("at_adpage_desktop.csv","at_adpage_desktop_not_desktop.csv","at_adpage_android.csv","at_adpage_ios.csv")
dev<-c("desktop","rwd","android","ios")
at_adpage <- data.frame()
for (i in seq_along(vec3) ) {
          read_file<- read.csv(vec3[i],sep = ";") %>%
                          mutate(device=dev[i]) %>%
                          select(-Pages)
                          colnames(read_file)<- c("dia","loads","device")
            at_adpage <- rbind(at_adpage, read_file)
}

#read MP file
mp_adpage<- read.csv("ad_page_mixpanel.csv",sep = ",")

#Convert from wide to long format like AT file
mp_adpage_l<-gather(mp_adpage,device, loads, rwd:desktop, factor_key = T )
names(mp_adpage_l)<- c("dia","device","loads")

#rbind before and after periods
at_adpage$dia<- as.Date(at_adpage$dia,format = "%d/%m/%Y")
adpage<- rbind(at_adpage, mp_adpage_l)

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
                        facet_wrap(~device)

# align axis and build final graph -----------
grid.arrange(adpageWeekly, adpageWeeklyDevice, nrow=2, ncol=1) 

