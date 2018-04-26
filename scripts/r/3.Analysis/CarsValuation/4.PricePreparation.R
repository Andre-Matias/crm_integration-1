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


# Convert prices in local currency where needed -------------------------------

fExchangeRates <-
  function(dfFunction){
    dfFunction <- as.data.table(dfFunction)
    
    dfFunction$created_at_first_date <- 
      as.Date(dfFunction$created_at_first)

    colnames(dfHistoricalExchangeRates) <- 
      c("created_at_first_date", "PLN.EUR")
    
    dfFunction$priceValue <- as.numeric(as.character(dfFunction$priceValue))
    
    # join wide dataset with exchange rates dataset
    dfFunction <-
      dfFunction %>%
      left_join(dfHistoricalExchangeRates, 
                by = c("created_at_first_date"="created_at_first_date"))
    
    # currency conversion: if in eur convert to PLN, else leave it in PLN
    dfFunction[price_currency == "EUR", price_PLN := priceValue / PLN.EUR]
    dfFunction[price_currency == "PLN", price_PLN := priceValue ]
    
    # gross/net cases: if is gross leave it as it is, else if net multiply by 1.23
    dfFunction[price_gross_net == "gross", price_PLN := price_PLN]
    dfFunction[price_gross_net == "net", price_PLN := price_PLN * 1.23]
    return(dfFunction)

}
