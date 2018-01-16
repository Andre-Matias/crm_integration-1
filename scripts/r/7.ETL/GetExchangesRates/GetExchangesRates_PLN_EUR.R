# load libraries --------------------------------------------------------------
library("quantmod")
library("aws.s3")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#read historical exchange rates ----------------------------------------------
#  dfHistoricalExchangeRates <-
#   read.table(file = "rates_data.csv", header = TRUE, sep = ",")
# 
# dfHistoricalExchangeRates$Period.Start.Date <-
#   as.Date(dfHistoricalExchangeRates$Period.Start.Date)
# 
# names(dfHistoricalExchangeRates) <- c("date", "PLN.EUR")

# load historical data from aws S3 --------------------------------------------
dfHistoricalExchangeRates <-
  s3readRDS(object = "dfExchangeRates_PLN_EUR.rds",
            bucket = "pyrates-data-ocean/ETL/ExchangeRates/"
  )

# get actuals exchange rates from Oanda ---------------------------------------
getSymbols("PLN/EUR", src = "oanda", from = Sys.Date() - 180)

dfActualsExchangesRates <-
  data.frame(date = index(PLNEUR), PLNEUR, row.names = NULL)

# remove duplicates -----------------------------------------------------------

dfActualsExchangesRates <-
  dfActualsExchangesRates[!(dfActualsExchangesRates$date %in%
                              dfHistoricalExchangeRates$date), ]

# merge data.frames -----------------------------------------------------------
names(dfHistoricalExchangeRates) <- names(dfActualsExchangesRates)
dfExchangeRates <- rbind(dfHistoricalExchangeRates, dfActualsExchangesRates)

# save to S3 ------------------------------------------------------------------
s3saveRDS(x = dfExchangeRates, object = "dfExchangeRates_PLN_EUR.rds", 
          bucket = "pyrates-data-ocean/ETL/ExchangeRates/"
          )