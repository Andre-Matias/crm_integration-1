#' ################################################################################################
#' Valid only for Romania!
#' Price Preparation considering weekly exchange rates EUR to RON since some prices are in EUR currency
#' Also considering local VAT
#' 
#' ################################################################################################


# Load historical exchange rates from aws S3 -----------------------------------
dfHistoricalExchangeRates <- read_csv("rates_data.csv") # weekly rates (week start on Monday)

# make it available in S3
# dfHistoricalExchangeRates <-
#   as.data.table(
#     s3readRDS(object = "dfExchangeRates_RON_EUR.rds",
#             bucket = "pyrates-data-ocean/ETL/ExchangeRates/"
#   )
#   )


# Convert prices in local currency where needed -------------------------------

## calculate week number since exchange rates come weekly
dfCarsAdsWideWithPrice$week <- cut(as.Date(dfCarsAdsWideWithPrice$created_at_first), "week")

fExchangeRates <-
  function(dfFunction){
    dfFunction <- as.data.table(dfFunction)
    
    # dfFunction$created_at_first_date <- 
    #   as.Date(dfFunction$created_at_first)

    colnames(dfHistoricalExchangeRates) <- 
      c("week", "EUR.RON")
    
    dfFunction$priceValue <- as.numeric(as.character(dfFunction$priceValue))
    
    dfHistoricalExchangeRates$week <- as.Date(dfHistoricalExchangeRates$week)
    
    # join wide dataset with exchange rates dataset
    dfFunction <-
      dfFunction %>%
      left_join(dfHistoricalExchangeRates, 
                by = "week")
    
    # currency conversion: if in eur convert to PLN, else leave it in PLN
    dfFunction[price_currency == "EUR", price_RON := priceValue / EUR.RON]
    dfFunction[price_currency == "RON", price_RON := priceValue ]
    
    # gross/net cases: if is gross leave it as it is, else if net multiply by 1.19 (Romania VAT in 2017-2018= 19%)
    dfFunction[price_gross_net == "gross", price_RON := price_RON]
    dfFunction[price_gross_net == "net", price_RON := price_RON * 1.19]
    
    # remove currencies that are neither EUR nor RON!!!
    dfFunction <- subset(dfFunction, price_currency %in% c("EUR", "RON"))
    
    return(dfFunction)

}
