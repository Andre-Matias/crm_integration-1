# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

jqlQuery <- 
    '
    function main() {
      return Events({
        from_date: "2018-01-05",
        to_date:   "2018-12-01",
        event_selectors:[
          {event:"my_account"},
          {event:"my_ads_active"},
          {event:"my_ads_pending"},
          {event:"my_ads_closed"},
          {event:"my_ads_moderated"},
          {event:"my_ads_statistics"}
          ]
      })
      .groupBy([mixpanel.numeric_bucket("time", mixpanel.weekly_time_buckets), "distinct_id", "properties.business_status", "properties.platform", "name"], mixpanel.reducer.count());
    }
    '

result <- 
  mixpanelJQLQuery(account = mixpanelStandvirtualAccount, jqlQuery, columnNames = c("epoch_week", "distinct_id", "business_status", "platform", "event", "value"), toNumeric = c(6))

df <-
  result %>%
  mutate(week = anytime(as.numeric(epoch_week)/1000)) %>%
  spread(key = event,value = value)


df[is.na(df)] <- 0

dfStats <-
  df %>%
  mutate(unique_my_account_activity = ifelse((my_account+my_ads_active+my_ads_pending+my_ads_closed+my_ads_moderated+my_ads_statistics)>0,1,0),
         unique_my_ads_statistics = ifelse((my_ads_statistics)>0,1,0)
         )%>%
  group_by(platform, week) %>%
  summarise(users = sum(unique_my_account_activity), 
            users_view_stats = sum(unique_my_ads_statistics), 
            KR = sum(unique_my_ads_statistics) / sum(unique_my_account_activity)
            )#%>%
  #filter(business_status %in% c("business")) %>%
  #arrange(week, platform, business_status)

#otomoto
ggplot(dfStats[dfStats$platform=='desktop', ])+
  geom_line(aes(x = week, y = KR))+
  geom_point(aes(x = week, y = KR))+
  scale_x_datetime(date_breaks = "week", limits = c(as.POSIXct("2018-01-08"), as.POSIXct("2018-03-26")), date_labels = "%d\n%b\n%y")+
  scale_y_continuous(limits = c(0, 0.15), breaks = seq(0,0.15,0.025), labels = scales::percent)+
  ggtitle("KR2.2 - Increase 15% the number of sellers that see the ads stats", subtitle = "Otomoto - desktop only")+
  geom_hline(yintercept=0.145, color='coral', size=1)+
  theme_fivethirtyeight()

#autovit
ggplot(dfStats[dfStats$platform=='desktop', ])+
  geom_line(aes(x = week, y = KR))+
  geom_point(aes(x = week, y = KR))+
  scale_x_datetime(date_breaks = "week", limits = c(as.POSIXct("2018-01-08"), as.POSIXct("2018-03-26")), date_labels = "%d\n%b\n%y")+
  scale_y_continuous(limits = c(0, 0.15), breaks = seq(0,0.15,0.025), labels = scales::percent)+
  ggtitle("KR2.2 - Increase 15% the number of sellers that see the ads stats", subtitle = "Autovit - desktop only")+
  geom_hline(yintercept=0.107, color='coral', size=1)+
  theme_fivethirtyeight()

#standvirtual
ggplot(dfStats[dfStats$platform=='desktop', ])+
  geom_line(aes(x = week, y = KR))+
  geom_point(aes(x = week, y = KR))+
  scale_x_datetime(date_breaks = "week", limits = c(as.POSIXct("2018-01-08"), as.POSIXct("2018-03-26")), date_labels = "%d\n%b\n%y")+
  scale_y_continuous(limits = c(0, 0.15), breaks = seq(0,0.15,0.025), labels = scales::percent)+
  ggtitle("KR2.2\nIncrease 15% the number of sellers that see the ads stats", subtitle = "Standvirtual - desktop only")+
  geom_hline(yintercept=0.117, color='coral', size=1)+
  theme_fivethirtyeight()
