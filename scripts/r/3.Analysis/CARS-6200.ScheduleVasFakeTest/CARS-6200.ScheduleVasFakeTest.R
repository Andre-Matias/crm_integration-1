# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("showtext")
library("glue")
library("aws.s3")
library("lubridate")

# load configs --------------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

jqlQuery<- 
  '
    function main() {
      return Events({
        from_date: "2018-04-02",
        to_date:   "2018-04-08"
      })
      .filter(
        e => ["my_ads_1_click_vas_modal", "schedule_vas_button", "my_ads_1_click_vas_modal_confirm"].indexOf(e.name) >= 0
        && (
          e.properties.$referring_domain 
          && e.properties.$referring_domain.includes("fixeads") === false 
          || !e.properties.$referring_domain)
        )
      .groupByUser([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name"], mixpanel.reducer.count())
      .groupBy(["key.1", "key.2"], mixpanel.reducer.count())
      ;
    }
  '

dfAll <- as_tibble()

for(i in listAccounts){
  
  dfJqlQuery <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQuery, columnNames = c("epoch_day", "event", "quantity" ),
                       toNumeric = c(1, 3)
      )
    )
  
  dfJqlQuery$platform <- as.character(i[[1]])
  
  print(nrow(dfAll))
  print(as.character(i[[1]]))
  
  if(nrow(dfAll)==0){
    dfAll <- dfJqlQuery
  } else {
    dfAll <- rbind(dfAll, dfJqlQuery)
  }
  
}

dfDaily <-
  dfAll %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  spread(key = event, value = quantity) %>%
  mutate(CTR = schedule_vas_button / my_ads_1_click_vas_modal)

dfWeekly <-
  dfAll %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  group_by(platform, event) %>%
  summarise(quantity = sum(quantity, na.rm = TRUE)) %>%
  spread(key = event, value = quantity) %>%
  mutate(CTR = schedule_vas_button / my_ads_1_click_vas_modal)


ggplot(dfDaily)+
  geom_line(aes(x = day ,y = CTR, color = platform ))+
  geom_point(aes(x = day ,y = CTR, color = platform ))+
  geom_text(aes(x = day ,y = CTR, label = scales::percent(round(CTR, 3)), color = platform), vjust = -0.4)+
  scale_x_datetime(date_breaks = "day")+
  scale_y_continuous(limits = c(0, NA), labels = scales::percent)+
  ggtitle("Schedule VAS - Fake Test", subtitle = "")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfWeekly)+
  geom_bar( stat = "identity", aes(x = day ,y = CTR, color = platform ))+
  geom_text(aes(x = day ,y = CTR, label = scales::percent(round(CTR, 3)), color = platform), vjust = -0.4)+
  scale_x_datetime(date_breaks = "day")+
  scale_y_continuous(limits = c(0, NA), labels = scales::percent)+
  ggtitle("Schedule VAS - Fake Test", subtitle = "")+
  theme_fivethirtyeight(base_family = "opensans")
  
  