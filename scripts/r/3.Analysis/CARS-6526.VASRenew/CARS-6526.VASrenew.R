# load libraries --------------------------------------------------------------
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("ggplot2")
library("showtext")
library("glue")
library("aws.s3")
library("lubridate")
library("RMySQL")
library("scales")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

querySQl <-
"
SELECT id_ad, category_id, name, name_en, code, type, duration, price,
date, last_status_date, PS.status, PS.provider
FROM paidads_user_payments PUP
INNER JOIN paidads_indexes PI
ON PUP.id_index = PI.id
INNER JOIN payment_session PS
ON PUP.id_transaction = PS.id
INNER JOIN ads a
ON id_ad = a.id
WHERE 
a.created_at_first >= '2017-11-01 00:00:00'
AND a.category_id = 29
AND a.net_ad_counted = 1
AND type IN ('topads', 'highlight', 'ad_homepage', 'bump_up', 'ad_bighomepage')
AND PS.status = 'finished'
AND date >= '2017-11-01 00:00:00'
;
"

for(i in listAccounts){
  # connect to database  ------------------------------------------------------

  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(i[[3]]), 
      dbname = i[[4]]
    )
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  dbDisconnect(conDB)
  
dfSqlQuery_Stats <-
  dfSqlQuery %>%
  group_by(id_ad, type) %>% 
  summarise(qtyVASPerAd = sum(n())) %>%
  group_by(type, qtyVASPerAd) %>%
  summarise(qtyVASPerTypeFrequency = sum(n())) %>%
  mutate(shareVASPerTypeFrequency = qtyVASPerTypeFrequency / sum(qtyVASPerTypeFrequency))

g <-
  ggplot(dfSqlQuery_Stats)+
  geom_line(
    aes(x = qtyVASPerAd, y = shareVASPerTypeFrequency, group = type, color = type)
    )+
  scale_x_continuous(breaks = seq(0, 10,1), limits = c(0,10))+
  scale_y_continuous(breaks = seq(0, 1, 0.1),labels = scales::percent)+
  theme_fivethirtyeight(base_family = "opensans")+
  ggtitle("Quantity of VAS by Ad (% share)",
          subtitle = paste(i[[1]], "|| 2017-11-01 => 2018-05-01 ")
          )
  
ggsave(filename = paste0("~/tmp/", "ghQuantityOfVasByAd_",as.character(i[[1]]), ".png" ),
       g,
       width = 800/72, height = 450/72, dpi = 72, units = "in", device='png')

  s3saveRDS(x = dfSqlQuery, 
            bucket = bucket_path, 
            object = paste0("CARS/CARS-6526/VASbyAdId_", i[[4]], ".RDS")
  )
  
  s3saveRDS(x = dfSqlQuery_Stats, 
            bucket = bucket_path, 
            object = paste0("CARS/CARS-6526/VASbyAdId_Stats_", i[[4]], ".RDS")
  )
}