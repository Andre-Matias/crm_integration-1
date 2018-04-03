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
rm(list = ls(pattern = "df.*"))
font_add_google("Open Sans", "opensans")

# 
listgMixpanelAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount),
       autovitro = list("AutovitRO", mixpanelAutovitAccount),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount)
  )

jqlQuery <- 
  '
function main() {
return Events({
from_date: "2018-01-01",
to_date:   "2018-12-01"
})
.filter(e=>e.name.includes("my_ads_1_click_vas")===true &&
e.properties.$referring_domain != "myotomoto.fixeads.com" && 
e.properties.$referring_domain != "www.myotomoto.fixeads.com"
)
.groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets),"name", "properties.vas_type.0.0", "properties.f_order_id"],
mixpanel.reducer.count())
}
'

for(i in listgMixpanelAccounts){
print(class(i[[1]]))
rawResult <- 
  as_tibble(
    mixpanelJQLQuery(account = i[[2]], 
                     jqlQuery,
                     columnNames = c("epoch_day", "event", "vastype", "f_order_id", "quantity"),
                     toNumeric = c(1, 5)
                    )
  )

assign(paste0("orders_", as.character(i[[1]])), paste(unique(rawResult$f_order_id[!is.na(rawResult$f_order_id)]), collapse = ", "))


result <-
  rawResult %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  group_by(day, vastype, event) %>%
  summarise(quantity = sum(quantity)) %>%
  spread(key = event, value = quantity) %>%
  mutate(CTR = my_ads_1_click_vas_modal_confirm / my_ads_1_click_vas_select) %>%
  filter(!is.na(CTR))

assign(paste0("df", "JQLResult", as.character(i[[1]])), result)

ghFinishedFunnel <-
  ggplot(result)+
    geom_bar(stat="identity", aes(day, my_ads_1_click_vas_modal_confirm, group = vastype, fill = vastype))+
    scale_x_datetime(date_labels = "%d\n%b\n%y", date_breaks = "day")+
    scale_y_continuous(limits = c(0, NA))+
    xlab("")+
    ylab("")+
    ggtitle("1-Click-VAS | Finished Funnel", subtitle = as.character(i[[1]]))+
    theme_fivethirtyeight(base_family = "opensans")

ghFunnelConversion <-
  ggplot(result)+
    geom_line(stat="identity", aes(day, CTR, group=vastype, color=vastype))+
    scale_x_datetime(date_labels = "%d\n%b\n%y", date_breaks = "day")+
    scale_y_continuous(limits = c(0, NA), labels = scales::percent)+
    xlab("")+
    ylab("")+
    ggtitle("1-Click-VAS | CTR", subtitle = as.character(i[[1]]))+
    theme_fivethirtyeight(base_family = "opensans")

ggsave(filename = paste0("~/tmp/", "ghFinishedFunnel_",as.character(i[[1]]), ".png" ),
       ghFinishedFunnel,
       width = 800/72, height = 450/72, dpi = 72, units = "in", device='png')

ggsave(filename = paste0("~/tmp/", "ghFunnelConversion_",as.character(i[[1]]), ".png" ),
       ghFunnelConversion,
       width = 800/72, height = 450/72, dpi = 72, units = "in", device='png')
}

dbs <-
  list(
  c("OtomotoPL", 3317, "otomotopl"),
  c("AutovitRO", 3315, "autovitro"),
  c("StandvirtualPT", 3308, "carspt")
  )

endDate <- Sys.Date()

querySQl <-
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
  AND PI.type IN('bump_up', 'topads', 'ad_homepage')
  AND PUP.is_removed_from_invoice = 0
  AND price < 0
  AND PS.provider NOT IN('admin')
  AND PS.last_status_date < '{endDate}'
  AND PS.id IN({order})
  GROUP BY 1,2;
  "


for(vertical in dbs){
  print(vertical[1])
  print(vertical[2])
  print(vertical[3])
  
  print(paste0("orders_", vertical[1]))
        
  order <- get(paste0("orders_", vertical[1]))
          
  querySQl <- glue(querySQl)
  
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

  dbDisconnect(conDB)
  
  dfSqlQuery$Month <- as.POSIXct(dfSqlQuery$Month)
  
assign(paste0("dfQueryResults_", vertical[1]), value = dfSqlQuery)
  
  dfSqlQuery <- 
    dfSqlQuery %>% 
    mutate(Month = as.Date(as.POSIXct(paste0(substr(Month, 1, 8), "01"))))
  
  ghFinishedFunnel <-
    ggplot(data = dfSqlQuery)+
    geom_bar(stat="identity", aes( x = Month, y = DailyAvg, fill = type), position = "dodge")+
    geom_text(aes(x = Month, y = DailyAvg, label = DailyAvg, color = type, group = type), position = position_dodge(1), vjust = -.5)+
    scale_x_date(date_labels = "%d\n%b\n%y", date_breaks = "month", limits = c(as.Date("2018-03-01"), NA))+
    scale_y_continuous(limits = c(0, NA))+
    scale_fill_brewer(type = "qual", palette = 3, direction = 1)+
    scale_colour_brewer(type = "qual", palette = 3, direction = 1)+
    xlab("")+
    ylab("")+
    ggtitle("VAS | Daily Average Aquisition", subtitle = paste(as.character(vertical[1]), "(only: bump up, tops ads, ad_homepage)"))+
    theme_fivethirtyeight(base_family = "opensans")
  
  ggsave(filename = paste0("~/tmp/", "ghVASDailyAverage",as.character(vertical[1]), ".png" ),
         ghFinishedFunnel,
         width = 800/72, height = 450/72, dpi = 72, units = "in", device='png')

}