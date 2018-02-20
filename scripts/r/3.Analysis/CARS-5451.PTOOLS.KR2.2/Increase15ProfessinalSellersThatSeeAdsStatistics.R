# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

jqlQuery <- 
    '
    function main() {
      return Events({
        from_date: "2018-02-05",
        to_date:   "2018-02-11",
        event_selectors:[
          {event:"my_account"},
          {event:"my_ads_active"},
          {event:"my_ads_pending"},
          {event:"my_ads_closed"},
          {event:"my_ads_moderated"},
          {event:"my_ads_statistics"}
          ]
      })
      .groupBy(["distinct_id", "properties.business_status", "properties.platform", "name"], mixpanel.reducer.count());
    }
    '

result <- 
  mixpanelJQLQuery(mixpanelOtomotoAccount, jqlQuery, columnNames = c("distinct_id", "business_status", "platform", "event", "value"), toNumeric = c(5))

df <-
  result %>%
  spread(key = event,value = value)

df[is.na(df)] <- 0

df <-
  df %>%
  mutate(unique_my_account_activity = ifelse((my_account+my_ads_active+my_ads_pending+my_ads_closed+my_ads_moderated+my_ads_statistics)>0,1,0),
         unique_my_ads_statistics = ifelse((my_ads_statistics)>0,1,0)
         )%>%
  group_by(platform, business_status) %>%
  summarise(users = sum(unique_my_account_activity), users_view_stats = sum(unique_my_ads_statistics), KR = sum(unique_my_ads_statistics) / sum(unique_my_account_activity)) %>%
  filter(business_status %in% c("business", "private")) %>%
  arrange(platform, business_status)