#' ################################################################################################
#' Convert data types
#' 
#' ################################################################################################


#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

# df <- dfFunction

fDataTypes <-
  function(df){
    
    # select variables --------------------------------------------------------
    ## nr_seats, new_used parameters are missing in the dataset
    vars <- 
    c("ad_id", "created_at_first", "make", "model", "mileage", "year",
      "body_type", "engine_capacity", "engine_power", "gearbox",
      "fuel_type", "damaged", "price_RON")
    
    
    df <- df[, vars, with = FALSE]

    gc()
    
    # make --------------------------------------------------------------------
    df$make <- as.character(df$make)

    # model -------------------------------------------------------------------
    df$model <- as.character(df$model)

    # mileage -----------------------------------------------------------------
    df$mileage <- as.numeric(df$mileage)
    
    # year --------------------------------------------------------------------
    df$year <- as.numeric(as.character(df$year))

    # body_type ---------------------------------------------------------------
    df$body_type <- as.character(df$body_type)

    # engine_capacity ---------------------------------------------------------
    df$engine_capacity <- as.numeric(as.character(df$engine_capacity))
    
    # engine_power ------------------------------------------------------------
    df$engine_power <- as.numeric(as.character(df$engine_power))

    # gearbox -----------------------------------------------------------------
    df$gearbox <- as.character(df$gearbox)

    # nr_seats ----------------------------------------------------------------
    #df$nr_seats <- as.character(df$nr_seats)

    # new_used ----------------------------------------------------------------
    #df$new_used <- as.character(df$new_used)

    # fuel_type ---------------------------------------------------------------
    df$fuel_type <- as.character(df$fuel_type)

    # damaged -----------------------------------------------------------------
    df$damaged <- as.character(df$damaged)

    # price_RON ---------------------------------------------------------------
    df$price_RON <- as.numeric(as.character(df$price_RON))
    
    # create var age ----------------------------------------------------------
    df$age <- as.numeric(year(df$created_at_first)-df$year)
    
    #summary(df)
    #View(head(filter(df, is.na(engine_capacity)), 100))
    #map(df, ~sum(is.na(.)))
    # Do more investigation on why/which are the NAs? at first glance it seems they are present across multiple variables, no pattern
    # Shall we just keep cars that are not damaged?

    gc()
    
    return(df)
 }
