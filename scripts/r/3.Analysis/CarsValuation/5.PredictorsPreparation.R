#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

# configs 
options(scipen=9999)

# load data -------------------------------------------------------------------
dfCarsAdsWideWithPrice <-
  readRDS(file = "~/tmp/dfCarsAdsWideWithPrice.RDS")

# define outliers cut ---------------------------------------------------------

quantileDown <- 0.05
quantileUp <- 0.95

# choose only ads that were live ----------------------------------------------
dfCarsAdsWideWithPrice <- 
  dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$net_ad_counted == "1", ]

# remove damaged cars or unknown
dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$damaged == "0", ]

dfCarsAdsWideWithPrice$damaged <- 
  factor(dfCarsAdsWideWithPrice$damaged, levels = unique(dfCarsAdsWideWithPrice$damaged))

# get outliers values for price -----------------------------------------------
dfTmp <-
  dfCarsAdsWideWithPrice %>%
  group_by(make, model, damaged) %>%
  summarise(LowerValue = quantile(price_PLN,probs = c(quantileDown), na.rm = TRUE),
            UpperValue = quantile(price_PLN,probs = c(quantileUp), na.rm = TRUE),
            qty = sum(!is.na(ad_id))
            )%>%
  inner_join(dfCarsAdsWideWithPrice,
             by = c("make", "model", "damaged")) %>%
  mutate(outlier = ifelse(
    price_PLN <= LowerValue | price_PLN >= UpperValue, TRUE, FALSE)
  ) %>%
  filter(outlier == TRUE) %>%
  select(ad_id)

  
# price_PLN -------------------------------------------------------------------
#remove ads with price outliers
dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[!(dfCarsAdsWideWithPrice$ad_id %in% dfTmp$ad_id), ]

# make ------------------------------------------------------------------------
sum(is.na(dfCarsAdsWideWithPrice$make))

dfCarsAdsWideWithPrice$make <- 
  as.character(dfCarsAdsWideWithPrice$make)

sum(nchar(dfCarsAdsWideWithPrice$make) < 1)

dfCarsAdsWideWithPrice$make <- 
  factor(dfCarsAdsWideWithPrice$make, levels = unique(dfCarsAdsWideWithPrice$make))

# model
sum(is.na(dfCarsAdsWideWithPrice$model))

dfCarsAdsWideWithPrice$model <- 
  as.character(dfCarsAdsWideWithPrice$model)

dfCarsAdsWideWithPrice$model <- 
  factor(dfCarsAdsWideWithPrice$model, levels = unique(dfCarsAdsWideWithPrice$model))

df_model <-
  dfCarsAdsWideWithPrice %>%
  group_by(make, model) %>%
  summarise(qtyAds = sum(!is.na(ad_id))) %>%
  group_by() %>%
  arrange(desc(qtyAds)) %>%
  mutate(shareAds = qtyAds / sum(qtyAds), cumShareAds = cumsum(shareAds))

# version
dfCarsAdsWideWithPrice$version <- 
  as.character(dfCarsAdsWideWithPrice$version)

sum(is.na(dfCarsAdsWideWithPrice$version) 
    | nchar(dfCarsAdsWideWithPrice$version) <1)

# mileage 
dfCarsAdsWideWithPrice$mileage <-
  as.numeric(as.character(dfCarsAdsWideWithPrice$mileage))

sum(is.na(dfCarsAdsWideWithPrice$mileage))

# remove NA mileage -----------------------------------------------------------

dfCarsAdsWideWithPrice <- 
  dfCarsAdsWideWithPrice[!is.na(dfCarsAdsWideWithPrice$mileage), ]

dfTmp <-
  df %>%
  group_by(make, model) %>%
  summarise(LowerValue = quantile(mileage,probs = c(0.05), na.rm = TRUE),
            UpperValue = quantile(mileage,probs = c(0.95), na.rm = TRUE),
            qty = sum(!is.na(ad_id))
  )%>%
  inner_join(dfCarsAdsWideWithPrice,
             by = c("make", "model")) %>%
  mutate(outlier = ifelse(
    mileage <= LowerValue | mileage >= UpperValue, TRUE, FALSE)
  ) %>%
  filter(outlier == TRUE) %>%
  select(ad_id)

dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[!(dfCarsAdsWideWithPrice$ad_id %in% dfTmp$ad_id), ]

# year
dfCarsAdsWideWithPrice$year <-
  as.numeric(as.character(dfCarsAdsWideWithPrice$year))

dfCarsAdsWideWithPrice <-
dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$year >= 1990
                       & dfCarsAdsWideWithPrice$year <= year(Sys.Date()), ]

#age
dfCarsAdsWideWithPrice$age <-
as.numeric(year(dfCarsAdsWideWithPrice$created_at_first)-dfCarsAdsWideWithPrice$year)

# body_type
dfCarsAdsWideWithPrice$body_type <- 
  as.character(dfCarsAdsWideWithPrice$body_type)

dfCarsAdsWideWithPrice$body_type <- 
  factor(dfCarsAdsWideWithPrice$body_type, levels = unique(dfCarsAdsWideWithPrice$body_type))

#engine_capacity
dfCarsAdsWideWithPrice$engine_capacity <-
  as.numeric(as.character(dfCarsAdsWideWithPrice$engine_capacity))

dfTmp <-
  dfCarsAdsWideWithPrice %>%
  group_by(make, model) %>%
  summarise(LowerValue = round(quantile(engine_capacity,probs = c(0.005), na.rm = TRUE),0),
            UpperValue = round(quantile(engine_capacity,probs = c(0.999), na.rm = TRUE),0),
            qty = sum(!is.na(ad_id))
  )%>%
  inner_join(dfCarsAdsWideWithPrice,
             by = c("make", "model")) %>%
  mutate(outlier = ifelse(
    engine_capacity < LowerValue | engine_capacity > UpperValue, TRUE, FALSE)
  ) %>%
  filter(engine_capacity > 500 & engine_capacity < 7000) %>%
  filter(outlier == TRUE) %>%
  select(ad_id, make, model, engine_capacity, LowerValue, UpperValue)

dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[!(dfCarsAdsWideWithPrice$ad_id %in% dfTmp$ad_id), ]

#engine_power
dfCarsAdsWideWithPrice$engine_power <-
  as.numeric(as.character(dfCarsAdsWideWithPrice$engine_power))


dfTmp <-
  dfCarsAdsWideWithPrice %>%
  group_by(make, model) %>%
  summarise(LowerValue = quantile(engine_power,probs = c(0.01), na.rm = TRUE),
            UpperValue = quantile(engine_power,probs = c(0.995), na.rm = TRUE),
            qty = sum(!is.na(ad_id))
  )%>%
  inner_join(dfCarsAdsWideWithPrice,
             by = c("make", "model")) %>%
  mutate(outlier = ifelse(
    engine_power < LowerValue | engine_power > UpperValue, TRUE, FALSE)
  ) %>%
  filter(outlier == TRUE) %>%
  select(ad_id, model, engine_power, LowerValue, UpperValue)

dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[!(dfCarsAdsWideWithPrice$ad_id %in% dfTmp$ad_id), ]

#gearbox
dfCarsAdsWideWithPrice$gearbox <- 
  factor(dfCarsAdsWideWithPrice$gearbox, levels = unique(dfCarsAdsWideWithPrice$gearbox))

dfCarsAdsWideWithPrice <- 
  dfCarsAdsWideWithPrice[!(dfCarsAdsWideWithPrice$gearbox==""), ]

#nr_seats
dfCarsAdsWideWithPrice <-
  dfCarsAdsWideWithPrice[dfCarsAdsWideWithPrice$nr_seats!="", ]

dfCarsAdsWideWithPrice$nr_seats <- 
  factor(dfCarsAdsWideWithPrice$nr_seats, levels = unique(dfCarsAdsWideWithPrice$nr_seats))

#new_used
dfCarsAdsWideWithPrice$new_used <- 
  factor(dfCarsAdsWideWithPrice$new_used, levels = unique(dfCarsAdsWideWithPrice$new_used))

#fuel_type
dfCarsAdsWideWithPrice$fuel_type <- 
  factor(dfCarsAdsWideWithPrice$fuel_type, levels = unique(dfCarsAdsWideWithPrice$fuel_type))

dfInputForModel <-
  dfCarsAdsWideWithPrice[,
    c("make", "model", "mileage", "year", "age", "body_type","engine_capacity",
      "engine_power", "gearbox", "nr_seats", "new_used", "fuel_type", "damaged",
      "price_PLN")
    ]

dfInputForModel <- dfInputForModel[complete.cases(dfInputForModel),]

train_idx <- sample(x = nrow(dfInputForModel), size = 2/3 * nrow(dfInputForModel))
dfInputForModel_train <- dfInputForModel[train_idx, ]
dfInputForModel_test <- dfInputForModel[-train_idx, ]

dfInputForModel_train <-
  dfInputForModel_train %>%
  arrange(make, model, year, age, body_type, engine_capacity, engine_power,
          gearbox, nr_seats, new_used, fuel_type)

dfInputForModel_test <-
  dfInputForModel_test %>%
  arrange(make, model, year, age, body_type, engine_capacity, engine_power,
          gearbox, nr_seats, new_used, fuel_type)

saveRDS(dfInputForModel_train, "~/tmp/dfInputForModel_train.RDS")
saveRDS(dfInputForModel_test, "~/tmp/dfInputForModel_test.RDS")
