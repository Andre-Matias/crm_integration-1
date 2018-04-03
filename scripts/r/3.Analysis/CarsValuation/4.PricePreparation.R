#load librabries --------------------------------------------------------------
library("aws.s3")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")

# load credentials -------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# load historical data from aws S3 --------------------------------------------
dfHistoricalExchangeRates <-
  as.data.table(
    s3readRDS(object = "dfExchangeRates_PLN_EUR.rds",
            bucket = "pyrates-data-ocean/ETL/ExchangeRates/"
  )
  )

# load data -------------------------------------------------------------------

fExchangeRates <-
  function(dfFunction){
    dfFunction <- as.data.table(dfFunction)
    
    dfFunction$created_at_first_date <- 
      as.Date(dfFunction$created_at_first)

colnames(dfHistoricalExchangeRates) <- 
  c("created_at_first_date", "PLN.EUR")

dfFunction$priceValue <- as.numeric(as.character(dfFunction$priceValue))

dfFunction <-
  dfFunction %>%
  left_join(dfHistoricalExchangeRates, 
            by = c("created_at_first_date"="created_at_first_date"))

dfFunction[price_currency == "EUR", price_PLN := priceValue / PLN.EUR]
dfFunction[price_currency == "PLN", price_PLN := priceValue ]

dfFunction[price_gross_net == "gross", price_PLN := price_PLN]
dfFunction[price_gross_net == "net", price_PLN := price_PLN * 1.23]
return(dfFunction)

}
