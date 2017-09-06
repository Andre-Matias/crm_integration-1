Conlusion:
The cron to expiry the was not working.
When they turned it on, it 

# libraries -------------------------------------------------------------------
library("config")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")

# load db configurations ------------------------------------------------------
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "stradia_ar") )

# -----------------------------------------------------------------------------
load("~/personal_credentials.Rdata")

# config  variables -----------------------------------------------------------
range_start <- as.POSIXct("2017-08-30 00:00:00")
range_end <- as.POSIXct("2017-09-07 00:00:00")

# get data from ads history

library("RMySQL")

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= StradiaArDbPwd,  
                  host = "127.0.0.1", 
                  port = as.numeric(config$BiServerPort),
                  dbname = config$DbName
)



cmdSqlQuery <-
  "SELECT * FROM ads_history.ads_history_carsar WHERE `status` IS NOT NULL;"

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)

dbDisconnect(conDB)

dfRawAdsHistory <- dfQueryResults
rm("dfQueryResults")

dfAdsHistory <- dfRawAdsHistory[, c("primary","id","changed_at", "status")]

dfAdsHistory$changed_at<- fastPOSIXct(dfAdsHistory$changed_at)

dfAdsHistory <- arrange(dfAdsHistory, primary, id)

df <-
  dfAdsHistory %>%
  arrange(primary, id) %>%
  group_by(id) %>%
  mutate(
    next.id = lead(id, order_by=primary),
    next.primary = lead(primary, order_by=primary), 
    next.changed_at = lead(changed_at, order_by=primary), 
    next.status = lead(status, order_by=primary)
    ) %>%
  filter(status == "active")

df$next.changed_at[is.na(df$next.changed_at)] <- Sys.time()

daily <- seq(from = range_start,to = range_end, by='hour')

dfAll <- data.frame()

for (i in seq_along(daily)){
  
  start <- daily[[i]]
  end <- start + as.difftime(1, unit="hours")
  
  dfTmp <- df
  
  dfTmp$start <- start
  dfTmp$end <- end
  dfTmp$overlap <- 
    !(dfTmp$next.changed_at <= dfTmp$start 
    | dfTmp$changed_at >= dfTmp$end)
  
  if(nrow(dfAll) == 0){
    dfAll <- dfTmp
  } else {
    dfAll <- rbind(dfAll, dfTmp)
  }

}

dfAllStats <-
  dfAll %>%
  group_by(id, start, end) %>%
  summarise (ActiveAd = max(overlap)) %>%
  group_by(start, end) %>%
  summarise (qtyActiveAds = sum(ActiveAd))


ggplot(dfAllStats) +
  geom_line(aes(start, qtyActiveAds))+
  scale_y_continuous(limits = c(0,NA))+
  scale_x_datetime()

dfActiveAdsHigh <-
  dfAll %>%
  filter(start == '2017-08-31 12:00:00' & overlap == TRUE)

dfActiveAdsLow <-
  dfAll %>%
  filter(start == '2017-09-01 20:00:00' & overlap == TRUE)



  