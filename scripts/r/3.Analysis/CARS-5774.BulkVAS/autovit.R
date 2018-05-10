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

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
font_add_google("Open Sans", "opensans")

jqlQuery <- 
  '
  function main() {
    return Events({
  from_date: "2018-02-28",
  to_date:   "2018-06-02"
  })
  .filter(e=>e.name.includes("my_ads_bulk_vas")===true && 
    e.properties.$referring_domain != "www.myautovit.fixeads.com" &&
    e.properties.$referring_domain != "myautovit.fixeads.com"
  )
  .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets),"name", "properties.vas_code.0"],
  mixpanel.reducer.count())
  }
  '

result <- 
  mixpanelJQLQuery(account = mixpanelAutovitAccount, 
                   jqlQuery, 
                   columnNames = c("epoch_day", "event", "vas", "qty"), 
                   toNumeric = c(1,4)
                   )
df <-
  result %>%
  filter(!is.na(event) & !is.na(vas)) %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  spread(key = event,value = qty) %>%
  mutate(CTR = my_ads_bulk_vas_modal_confirm / my_ads_bulk_vas_select)%>%
  select(-epoch_day)

jqlQuery <- 
  '
function main() {
  return Events({
    from_date: "2018-02-28",
    to_date:   "2018-06-02"
  })
  .filter(e=>e.name.includes("my_ads_bulk_vas")===true &&
            e.properties.$referring_domain != "www.myautovit.fixeads.com" &&
            e.properties.$referring_domain != "myautovit.fixeads.com" &&
            e.name == "my_ads_bulk_vas_modal_confirm"
  )
  .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets),"name", "properties.vas_code.0",
            "properties.f_order_id"],
           mixpanel.reducer.count())
}
'


result2 <- 
  mixpanelJQLQuery(account = mixpanelAutovitAccount, 
                   jqlQuery
  )

orders <- 
  paste(unique(result2$V4), collapse = ", ")


querySQl <-
  "
  SELECT DATE(date) as Day, id_transaction,  COUNT(*)qty
  FROM paidads_user_payments PUP
  INNER JOIN paidads_indexes PI
  ON PUP.id_index = PI.id
  WHERE id_transaction IN ({orders})
  GROUP  BY 1, 2;
  "
querySQl <- 
  glue(querySQl)


querySQl2 <-
  "
    SELECT LAST_DAY(PUP.date)Month, PI.type, COUNT(*)Qty, MAX(DAY(date))Days, ROUND(COUNT(*) / MAX(DAY(date)),0)DailyAvg
    FROM paidads_user_payments PUP
    INNER JOIN paidads_indexes PI
    ON PUP.id_index=PI.id
    INNER JOIN
    payment_session PS
    ON PUP.id_transaction=PS.id
    WHERE
    PUP.date >= '2017-09-01 00:00:00'
    AND PS.status = 'finished'
    AND PI.type IN('bump_up')
    AND PUP.is_removed_from_invoice = 0
    AND price < 0
    AND PS.provider NOT IN('admin')
    GROUP BY 1,2;
  "

dbs <-
  list(
    #c("AutovitRO", 3317, "AutovitRO")
    c("AutovitRO", 3315, "autovitro")
    #c("StandvirtualPT", 3308, "carspt")
  )


for(vertical in dbs){
  print(vertical[1])
  print(vertical[2])
  print(vertical[3])
  
  # connect to database  ------------------------------------------------------
  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(vertical[2]), 
      dbname = vertical[3]
    )
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  assign(paste0("dfQueryResults_", vertical[1]), value = dfSqlQuery)
  
  dfSqlQuery2 <-
    dbGetQuery(conDB, querySQl2)
  
  assign(paste0("dfQueryResults2_", vertical[1]), value = dfSqlQuery2)
  
  dbDisconnect(conDB)
}

dfQueryResults_AutovitRO_Stats <-
  dfQueryResults_AutovitRO %>%
  group_by(Day) %>%
  summarise(qtyVAS = sum(qty),
            meanVAS = mean(qty),
            qtyBaskets = sum(n())
            ) %>%
  mutate(Day = as.POSIXct(Day))

ggplot(df)+
  geom_bar(stat="identity", aes(x = day,y = my_ads_bulk_vas_select), fill = "navyblue")+
  geom_text(aes(x = day, y = my_ads_bulk_vas_select, label = my_ads_bulk_vas_select), color = "navyblue", vjust  = - 0.5 )+
  geom_bar(stat="identity", aes(x = day,y = my_ads_bulk_vas_modal_confirm), fill = "lightgreen")+
  geom_text(aes(x = day, y = my_ads_bulk_vas_modal_confirm, label = my_ads_bulk_vas_modal_confirm), color = "darkgreen", vjust  = 1.1 )+
  scale_y_continuous()+
  scale_x_datetime(date_breaks = "7 day", date_labels = "%d\n%b\n%y", limits = c(as.POSIXct("2018-02-28"), as.POSIXct("2018-05-08")))+
  ggtitle("Bump Bulk VAS - Started vs Finished Process", subtitle = "AUTOVIT (01/mar - 08/may/18)")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(df)+
  geom_line(aes(x = day,y = CTR))+
  geom_label(aes(x = day,y = CTR, label = scales::percent(round(CTR,2))))+
  scale_y_continuous(limits = c(0,1), labels = scales::percent)+
  scale_x_datetime(date_breaks = "7 day", date_labels = "%d\n%b\n%y", limits = c(as.POSIXct("2018-02-28"), as.POSIXct("2018-05-08")))+
  ggtitle("Bump Bulk VAS Convertion Rate", subtitle = "AUTOVIT (01/mar - 08/may/18)")+
  theme_fivethirtyeight(base_family = "opensans")

ggplot(dfQueryResults_AutovitRO_Stats)+
  geom_bar(stat="identity", aes(x = Day,y = qtyVAS), fill = "navyblue")+
  geom_text(aes(x = Day, y = qtyVAS, label = qtyVAS), color = "navyblue", vjust  = - 0.5 )+
  scale_y_continuous()+
  scale_x_datetime(date_breaks = "7 day", date_labels = "%d\n%b\n%y", limits = c(as.POSIXct("2018-02-28"), as.POSIXct("2018-05-08")))+
  ggtitle("Bump Bulk VAS - Total Quantity", subtitle = "AUTOVIT (01/mar - 08/may/18)")+
  theme_fivethirtyeight(base_family = "opensans")

dfQueryResults3_AutovitRO <- 
  dfQueryResults2_AutovitRO %>% 
  mutate(Month = as.POSIXct(paste0(substr(Month, 1, 8), "01")))


ggplot(dfQueryResults3_AutovitRO)+
  geom_bar(stat="identity", aes(x = Month,y = DailyAvg), fill = "navyblue")+
  geom_text(aes(x = Month, y = DailyAvg, label = DailyAvg), color = "navyblue", vjust  = - 0.5 )+
  scale_x_datetime(date_breaks = "month", date_labels = "%b\n%y")+
  scale_y_continuous(limits = c(0, 125))+
  ggtitle("VAS - Bump Up - Daily Average Quantity", subtitle = "AUTOVIT (01/mar - 08/may/18)")+
  theme_fivethirtyeight(base_family = "opensans")