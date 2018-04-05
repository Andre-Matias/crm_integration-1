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
library("ggplot2")

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
      .groupByUser([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name", "properties.experiments"], mixpanel.reducer.count())
      .groupBy(["key.1", "key.2", "key.3"], mixpanel.reducer.count())
      ;
    }
  '

dfAll <- as_tibble()

for(i in listAccounts){
  
  dfJqlQuery <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQuery, columnNames = c("epoch_day", "event", "experiment_id", "quantity"),
                       toNumeric = c(1, 4)
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
  filter( experiment_id %in% c("10491163383:10494470283", "10514851624:10514851625", "10516481454:10522071191")) %>%
  spread(key = event, value = quantity) %>%
  mutate(CTR = schedule_vas_button / my_ads_1_click_vas_modal)

dfWeekly <-
  dfAll %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  filter( experiment_id %in% c("10491163383:10494470283", "10514851624:10514851625", "10516481454:10522071191")) %>%
  group_by(platform, event) %>%
  spread(key = event, value = quantity) %>%
  filter(!is.na(schedule_vas_button)) %>%
  summarise(my_ads_1_click_vas_modal = sum(my_ads_1_click_vas_modal, na.rm = TRUE),
            my_ads_1_click_vas_modal_confirm = sum(my_ads_1_click_vas_modal_confirm, na.rm = TRUE),
            schedule_vas_button = sum(schedule_vas_button, na.rm = TRUE)
            ) %>%
  mutate(CTR = schedule_vas_button / my_ads_1_click_vas_modal)

s3saveRDS(x = dfDaily,
          object = "dfDaily.RDS",
          bucket = "pyrates-data-ocean/CARS-6200"
          )

s3saveRDS(x = dfWeekly,
          object = "dfWeekly.RDS",
          bucket = "pyrates-data-ocean/CARS-6200"
          )


