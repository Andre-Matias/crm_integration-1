# config

cfgLocation <- 
  "~/tmp/"

cfgDatasetName <-
  "RawHistoricalAds" # RawHistoricalAds # RealTestRawAds

cfgSite <-
  "OTO"

cfgFileSource <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, ".RDS")

cfgFileParams <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_params_wide", ".RDS")

cfgFileFeatures <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_features_wide", ".RDS")

cfgFileMain <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main", ".RDS")

cfgFileMainAIO <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main_AIO", ".RDS")

cfgFileMainAIOwPrice <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main_AIO_wPrice", ".RDS")

cfgFileMainAIOwPriceF <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main_AIO_wPriceF", ".RDS")

cfgFileMainAIOwPriceFM <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main_AIO_wPriceFM", ".RDS")

cfgFileMainAIOwPriceFMO <- 
  paste0(cfgLocation, cfgDatasetName, "_", cfgSite, "_main_AIO_wPriceFMO", ".RDS")

# source("2.ExtractParamsAndFeatures.R")
  
rawData <- readRDS(cfgFileSource)

source("2.ExtractParamsAndFeatures_Fast.R")

rt0 <- as.POSIXct(Sys.time())
df <- fExtract(rawData)
as.POSIXct(Sys.time()) - rt0


dfFeaturesWide <- as.data.table(df$features)
saveRDS(object = dfFeaturesWide, file = cfgFileFeatures)
rm(dfFeaturesWide)
gc()

dfParamsWide <- as.data.table(df$params)
saveRDS(object = dfParamsWide, file = cfgFileParams )
rm(dfParamsWide)
gc()

saveRDS(object = rawData %>% select(-params), file = cfgFileMain)
rm(rawData)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()

source("3.ToWideParameters.R")

dfFileMainAIO <-
  fAllInOne(dfFeaturesWide = readRDS(cfgFileFeatures),
            dfParamsWide = readRDS(cfgFileParams), 
            dfMainAds = readRDS(cfgFileMain)
  )

saveRDS(object = dfFileMainAIO, file = cfgFileMainAIO)
rm(dfFileMainAIO)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()

source("4.PricePreparation.R")
dfFileMainAIOwPrice <-
  fExchangeRates(readRDS(cfgFileMainAIO)
  )

print(paste("Listings with Normalized price", nrow(dfFileMainAIOwPrice)))
vars <- 
  c("ad_id", "make", "model", "mileage", "year", 
    "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    "new_used", "fuel_type", "damaged", "price_PLN")
print(paste("Complete Cases", sum(complete.cases(dfFileMainAIOwPrice[, vars, with=FALSE]))))
sapply(dfFileMainAIOwPrice[, vars, with=FALSE], function(x) sum(is.na(x)))

saveRDS(object = dfFileMainAIOwPrice, file = cfgFileMainAIOwPrice)

rm(dfFileMainAIOwPrice)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()
 
#### patch only historical data -----------------------------------------------

source("5.PredictorsPreparation_DataTypes.R")
dfFileMainAIOwPriceF <-
  fDataTypes(readRDS(cfgFileMainAIOwPrice))

#-------------------
  

print(paste("Listings with Normalized price", nrow(dfFileMainAIOwPriceF)))
vars <- 
  c("ad_id", "make", "model", "mileage", "year", 
    "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    "new_used", "fuel_type", "damaged", "price_PLN")
print(paste("Complete Cases", sum(complete.cases(dfFileMainAIOwPriceF[, vars, with=FALSE]))))

sapply(dfFileMainAIOwPriceF[, vars, with=FALSE], function(x) sum(is.na(x)))
  
#--------------------

saveRDS(object = dfFileMainAIOwPriceF, file = cfgFileMainAIOwPriceF)
rm(dfFileMainAIOwPriceF)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()

#-------------------------------------------------------------------------------
source("5.PredictorsPreparation_MissingValues.R")
dfFileMainAIOwPriceFM <-
  fMissingValues(readRDS(cfgFileMainAIOwPriceF))


#-------------------
  
print(paste("Listings with Normalized price", nrow(dfFileMainAIOwPriceFM)))

vars <- 
  c("ad_id", "make", "model", "mileage", "year", 
    "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    "new_used", "fuel_type", "damaged", "price_PLN")
print(paste("Complete Cases", sum(complete.cases(dfFileMainAIOwPriceFM[, vars, with=FALSE]))))

sapply(dfFileMainAIOwPriceFM[, vars, with=FALSE], function(x) sum(is.na(x)))

#--------------------

saveRDS(object = dfFileMainAIOwPriceFM, file = cfgFileMainAIOwPriceFM)
rm(dfFileMainAIOwPriceFM)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()

#-------------------------------------------------------------------------------

source("5.PredictorsPreparation_Outliers.R")
dfFileMainAIOwPriceFMO <-
  fOutliers(readRDS(cfgFileMainAIOwPriceFM))

#-------------------
  
print(paste("Listings with Normalized price", nrow(dfFileMainAIOwPriceFMO)))

vars <- 
  c("ad_id", "make", "model", "mileage", "year", 
    "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    "new_used", "fuel_type", "damaged", "price_PLN")
print(paste("Complete Cases", sum(complete.cases(dfFileMainAIOwPriceFMO[, vars, with=FALSE]))))

sapply(dfFileMainAIOwPriceFMO[, vars, with=FALSE], function(x) sum(is.na(x)))

#--------------------
  
saveRDS(object = dfFileMainAIOwPriceFMO, file = cfgFileMainAIOwPriceFMO)
rm(dfFileMainAIOwPriceFMO)
gc()

rm(list = setdiff(ls(), ls(pattern = "cfg")))
gc()

