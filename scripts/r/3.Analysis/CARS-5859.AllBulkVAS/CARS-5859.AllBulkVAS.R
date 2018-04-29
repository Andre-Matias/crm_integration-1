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

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

startDate <- as.character("2018-03-01")
endDate <- as.character(Sys.Date()-1)

listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

jqlQueryNotBump <- 
  '
    function main() {{
    return Events({{
    from_date: "{startDate}",
    to_date:   "{endDate}"
    }})
    .filter(e => e.name.includes("my_ads_bulk_vas_modal")
            && (["topads", "bump_up", "highlight", "ad_homepage", "ad_bighomepage"].indexOf(e.properties.vas_type) >= 0)
    )
    .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name", "properties.f_order_id", "properties.vas_type"], mixpanel.reducer.count());
    }}
  '

jqlQueryBump <- 
  '
    function main() {{
    return Events({{
    from_date: "{startDate}",
    to_date:   "{endDate}"
    }})
    .filter(e => e.name.includes("my_ads_bulk_vas_modal")
    // && e.properties.vas_type == "bump_up"
    )
    .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name", "properties.f_order_id", "properties.vas_type.0"], mixpanel.reducer.count());
    }}
  '

jqlQueryNotBump <-
  glue(jqlQueryNotBump)

jqlQueryBump <-
  glue(jqlQueryBump)


dfVAS_all <- as_tibble()
dfOrdersAll <- as_tibble()
dfFinished_all <- as_tibble()

for(i in listAccounts){
  
  dfJqlQueryNotBump <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQueryNotBump,
                       columnNames = c("epoch_day", "event", "f_order_id", "vas_type", "quantity"),
                       toNumeric = c(1, 3, 5) 
      )
    )

  dfJqlQueryBump <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQueryBump,
                       columnNames = c("epoch_day", "event", "f_order_id", "vas_type", "quantity"),
                       toNumeric = c(1, 3, 5) 
      )
    )
  
  dfVAS <- rbind(dfJqlQueryNotBump, dfJqlQueryBump)
  
  assign(x = paste0("dfVAS_", as.character(i[[1]])),
         value = dfVAS
  )
  
  dfVAS$project <- as.character(i[[1]])

  orders <- 
    paste(unique(dfVAS$f_order_id[!is.na(dfVAS$f_order_id)]), collapse = ", ")
  
  # connect to database  ------------------------------------------------------
  
  querySQl <-
    
  "
  SELECT id_transaction, status, COUNT(*) AS Quantity
  FROM paidads_user_payments PUP
  INNER JOIN payment_session PS
  ON PUP.id_transaction = PS.id
  WHERE PS.id IN ({orders})
  GROUP BY 1, 2
  ;
  "
  
  querySQl <- 
    glue(querySQl)
  
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
    
    dfSqlQuery$project <- as.character(i[[1]])
    
    if(nrow(dfVAS_all) == 0){
      dfVAS_all <- dfVAS
    } else {
      dfVAS_all <- rbind(dfVAS_all, dfVAS)
    }
    
    if(nrow(dfOrdersAll) == 0){
      dfOrdersAll <- dfSqlQuery
    } else {
      dfOrdersAll <- rbind(dfOrdersAll, dfSqlQuery)
    }
    
    dfFinished <-
      dfVAS %>%
      left_join(dfSqlQuery, by = c("f_order_id" = "id_transaction"))
    
    if(nrow(dfFinished_all) == 0){
      dfFinished_all <- dfFinished
    } else {
      dfFinished_all <- rbind(dfFinished_all, dfFinished)
    }  

}

df <-
  dfVAS_all %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  mutate(week = floor_date(day, unit = "week")) %>%
  group_by(project, day, vas_type, event) %>%
  summarise(totalQuantity = sum(quantity, na.rm = TRUE)) %>%
  spread(key = event, value = totalQuantity)

dfStats <-
  dfFinished_all %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  mutate(week = floor_date(day, unit = "week")) %>%
  group_by(project.x, week, vas_type, event) %>%
  summarise(quantity = sum(quantity)) %>%
  spread(key = event, value = quantity, fill = 0) %>%
  mutate(CTR = my_ads_bulk_vas_modal_confirm / my_ads_bulk_vas_modal)

dfStats2 <-
  dfFinished_all %>%
  mutate(day = anytime(as.numeric(epoch_day) / 1000)) %>%
  select(-epoch_day) %>%
  filter(status == "finished", event == "my_ads_bulk_vas_modal_confirm", !is.na(vas_type)) %>%
  mutate(week = floor_date(day, unit = "week")) %>%
  group_by(project.x, week, vas_type) %>%
  summarise(quantity = sum(Quantity))

ggplot(dfStats[dfStats$project.x == "OtomotoPL", ])+
  geom_line(aes(x = week, y = CTR, color = vas_type, group = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  scale_y_continuous(labels = scales::percent)+
  ggtitle("Bulk VAS Conversion Rate", subtitle = "Otomoto PL")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfStats2[dfStats2$project.x == "OtomotoPL", ])+
  geom_line(aes(x = week, y = quantity, color = vas_type))+
  geom_point(aes(x = week, y = quantity, color = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  ggtitle("Bulk VAS Quantity Sold", subtitle = "Otomoto PL")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfStats[dfStats$project.x == "AutovitRO", ])+
  geom_line(aes(x = week, y = CTR, color = vas_type, group = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  scale_y_continuous(labels = scales::percent)+
  ggtitle("Bulk VAS Conversion Rate", subtitle = "Autovit RO")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfStats2[dfStats2$project.x == "AutovitRO", ])+
  geom_line(aes(x = week, y = quantity, color = vas_type))+
  geom_point(aes(x = week, y = quantity, color = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  ggtitle("Bulk VAS Quantity Sold", subtitle = "Autovit RO")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfStats[dfStats$project.x == "StandvirtualPT", ])+
  geom_line(aes(x = week, y = CTR, color = vas_type, group = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  scale_y_continuous(labels = scales::percent)+
  ggtitle("Bulk VAS Conversion Rate", subtitle = "StandvirtualPT")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfStats2[dfStats2$project.x == "StandvirtualPT", ])+
  geom_line(aes(x = week, y = quantity, color = vas_type))+
  geom_point(aes(x = week, y = quantity, color = vas_type))+
  scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%Y")+
  ggtitle("Bulk VAS Quantity Sold", subtitle = "Standvirtual PT")+
  theme_fivethirtyeight(base_family = "opensans")



  
  
  
  
  
  