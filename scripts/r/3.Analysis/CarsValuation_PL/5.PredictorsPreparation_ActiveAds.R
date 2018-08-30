#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

# configs 
options(scipen=9999)


dfCarsAdsWideWithPrice <-
  readRDS(file = "~/tmp/otomoto_active_ads_wide.RDS")

dfCarsAdsWideWithPrice <- dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$ad_id > 6009725991, ]


dfCarsAdsWideWithPrice <- dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$`price\\[currency\\]` == "PLN", ]
dfCarsAdsWideWithPrice <- dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$`price\\[gross_net\\]` == "gross", ]

dfCarsAdsWideWithPrice$price_PLN <- as.numeric(dfCarsAdsWideWithPrice$priceValue)

dfCarsAdsWideWithPrice <- dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$damaged==0, ]

dfCarsAdsWideWithPrice$make <- factor(dfCarsAdsWideWithPrice$make, levels = unique(dfCarsAdsWideWithPrice$make))
dfCarsAdsWideWithPrice$model <- factor(dfCarsAdsWideWithPrice$model, levels = unique(dfCarsAdsWideWithPrice$model))
dfCarsAdsWideWithPrice$mileage <- as.numeric(as.character(dfCarsAdsWideWithPrice$mileage))
dfCarsAdsWideWithPrice$year <- as.numeric(as.character(dfCarsAdsWideWithPrice$year))

dfCarsAdsWideWithPrice <- dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$year <=2017, ]

dfCarsAdsWideWithPrice$body_type <- factor(dfCarsAdsWideWithPrice$body_type, levels = unique(dfCarsAdsWideWithPrice$body_type))
dfCarsAdsWideWithPrice$engine_capacity <- as.numeric(as.character(dfCarsAdsWideWithPrice$engine_capacity))
dfCarsAdsWideWithPrice$engine_power <- as.numeric(as.character(dfCarsAdsWideWithPrice$engine_power))
dfCarsAdsWideWithPrice$gearbox <- factor(dfCarsAdsWideWithPrice$gearbox, levels = unique(dfCarsAdsWideWithPrice$gearbox))
dfCarsAdsWideWithPrice$nr_seats <- factor(dfCarsAdsWideWithPrice$nr_seats, levels = unique(dfCarsAdsWideWithPrice$nr_seats))
dfCarsAdsWideWithPrice$new_used <- factor(dfCarsAdsWideWithPrice$new_used, levels = unique(dfCarsAdsWideWithPrice$new_used))
dfCarsAdsWideWithPrice$fuel_type <- factor(dfCarsAdsWideWithPrice$fuel_type, levels = unique(dfCarsAdsWideWithPrice$fuel_type))
dfCarsAdsWideWithPrice$damaged <- factor(dfCarsAdsWideWithPrice$damaged, levels = unique(dfCarsAdsWideWithPrice$damaged))
dfCarsAdsWideWithPrice$priceValue <- as.numeric(dfCarsAdsWideWithPrice$priceValue)

#age
dfCarsAdsWideWithPrice$age <-
  as.numeric(year(as.Date(dfCarsAdsWideWithPrice$created_at_first))-as.numeric(dfCarsAdsWideWithPrice$year))

dfCarsAdsWideWithPrice$age <- as.numeric(as.character(dfCarsAdsWideWithPrice$age))

#



dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[,
                         c("make", "model", "mileage", "year", "age", "body_type","engine_capacity",
                           "engine_power", "gearbox", "nr_seats", "new_used", "fuel_type", "damaged",
                           "price_PLN")
                         ]



dfCarsAdsWideWithPrice_ActiveAds <-
  dfCarsAdsWideWithPrice %>%
  arrange(make, model, year, age, body_type, engine_capacity, engine_power,
          gearbox, nr_seats, new_used, fuel_type)

saveRDS(dfCarsAdsWideWithPrice_ActiveAds, "~/tmp/dfCarsAdsWideWithPrice_ActiveAds.RDS")
