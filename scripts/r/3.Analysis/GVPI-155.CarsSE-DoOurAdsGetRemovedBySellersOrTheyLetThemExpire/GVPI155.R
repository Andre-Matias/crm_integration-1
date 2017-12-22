# libraries -------------------------------------------------------------------
library("config")
library("RMySQL")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("stringr")
library("ggthemes")
library("scales")
library("lubridate")

#for(site in c("standvirtual_pt", "otomoto_pl", "autovit_ro")){

for(site in c("autovit_ro")){

# load db configurations ------------------------------------------------------
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", site)
                      )

# -----------------------------------------------------------------------------
load("~/credentials.Rdata")

# get data --------------------------------------------------------------------
conDB<- 
  dbConnect(MySQL(), 
            user= config$DbUser, 
            password= bi_team_pt_password,  
            host = config$DbHost,
            port = as.numeric(config$DbPort),
            dbname = config$DbName
  )

if(site == "standvirtual_pt"){
  not_renewing_reasons_colname  <- "name_pt"
} else if(site == "autovit_ro"){
  not_renewing_reasons_colname  <- "name_ro"
} else if(site == "otomoto_pl"){
  not_renewing_reasons_colname <- "name_pl"
}

print(not_renewing_reasons_colname)

cmdSqlQuery <- 
  "
  SELECT A.id, A.created_at_first, A.category_id, A.status, A.reason_id, U.is_business
  FROM ads A
  INNER JOIN users U
  ON A.user_id = U.id
  WHERE created_at_first BETWEEN '2017-01-01 00:00:00' AND '2017-11-30 23:59:59'
  AND net_ad_counted = 1
  ;
  "

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)
dfAds <- as_tibble(dfQueryResults)

cmdSqlQuery <- 
  paste(
  "SELECT id,", not_renewing_reasons_colname,"AS name_en FROM not_renewing_reasons;"
  )

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)
dfNotRenewingReasons <- as_tibble(dfQueryResults)

cmdSqlQuery <- 
  "
  SELECT * FROM categories
  ;
  "

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)
dfCategories <- as_tibble(dfQueryResults)

dbDisconnect(conDB)

rm("dfQueryResults")


dfNotRenewingReasons$name_en <-
  gsub("Dei o ve\xedculo como retoma", "Dei o veículo como retoma", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Dei o ve\xedculo para abate", "Dei o veículo para abate", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("J\xe1 n\xe3o estou interessado em vender", "Já não estou interessado em vender", dfNotRenewingReasons$name_en)


dfNotRenewingReasons$name_en <-
  gsub("Brak zainteresowania og\\?oszeniem", "Lack of interest in the announcement", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Inny pow\\\xf3d", "Other reason", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Sprzeda\\?em dzi\\?ki otoMoto", "I sold thanks to otoMoto", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Sprzeda\\?em w innym serwisie", "I sold in another website", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Og\\?oszenie nieaktualne", "The advertisement is out of date", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Urlop, przerwa", "Leave, break", dfNotRenewingReasons$name_en)


dfNotRenewingReasons$name_en <-
  gsub("Am vandut prin Autovit.ro", "I sold through Autovit.ro", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Am vandut printr-un alt site", "I sold through another site", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Nu am primit solicitari si vreau sa renunt", "I did not receive requests and I want to quit", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Nu mai vreau sa vand, m-am razgandit", "I do not want to sell anymore, I changed my mind", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Il dezactivez doar temporar, voi reveni", "I only temporarily disable it, I will return", dfNotRenewingReasons$name_en)

dfNotRenewingReasons$name_en <-
  gsub("Alt motiv", "Another reason", dfNotRenewingReasons$name_en)


# Brak zainteresowania og?oszeniem || Brak zainteresowania ogłoszeniem || Lack of interest in the announcement
# Inny pow\xf3d || Inny  || Other reason
# Sprzeda?em dzi?ki otoMoto  || Sprzedałem dzięki otoMoto || I sold thanks to otoMoto
# Sprzeda?em w innym serwisie || Sprzedałem w innym serwisie || I sold in another website
# Og?oszenie nieaktualne || Ogłoszenie nieaktualne || The advertisement is out of date
# Urlop, przerwa || Leave, break

#
#
#
#


dfAll <-
  dfAds %>%
  left_join(dfCategories, by = c("category_id"="id")) %>%
  left_join(dfNotRenewingReasons, by = c("reason_id"="id"))

dfStatusStats <-
  dfAll %>%
  mutate(created_at_first = fastPOSIXct(created_at_first)) %>%
  mutate(created_at_first_month = floor_date(created_at_first, "month")) %>%
  filter(created_at_first <= "2017-11-30 23:59:59"
         & category_id == 29) %>%
  group_by(created_at_first_month, status) %>%
  summarise(qtyListings = sum(n())) %>%
  mutate(perListings = round(qtyListings / sum(qtyListings), 3))

dfRemovedByUserStats <-
  dfAll %>%
  mutate(created_at_first = fastPOSIXct(created_at_first)) %>%
  mutate(created_at_first_month = floor_date(created_at_first, "month")) %>%
  filter(created_at_first <= "2017-11-30 23:59:59"
         & category_id == 29
         & status == "removed_by_user") %>%
  group_by(created_at_first_month, name_en.y) %>%
  summarise(qtyListings = sum(n())) %>%
  mutate(perListings = round(qtyListings / sum(qtyListings), 3))

ghStatusStats <-
  ggplot(dfStatusStats)+
  geom_line(aes(x=created_at_first_month, y=perListings, color=status))+
  scale_x_datetime(date_breaks = "month", date_labels = "%b-%y")+
  scale_y_continuous(breaks = seq(0,1,0.05), labels = percent)+
  ggtitle(paste(site, "listings status by month of creation"))+
  theme_fivethirtyeight()
  

ghRemovedByUserStats <-
  ggplot(dfRemovedByUserStats)+
  geom_line(aes(x=created_at_first_month, y=perListings, color=name_en.y))+
  scale_x_datetime(date_breaks = "month", date_labels = "%b-%y")+
  scale_y_continuous(breaks = seq(0,1,0.05), labels = percent)+
  ggtitle(paste(site, "listings removal reasons"))+
  theme_fivethirtyeight()
}



