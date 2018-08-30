#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

fDataTypes <-
  function(df){
    # select variables --------------------------------------------------------
    
    vars <- 
    c("ad_id", "created_at_first", "make", "model", "mileage", "year",
      "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
      "new_used", "fuel_type", "damaged", "price_PLN")
    
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
    df$nr_seats <- as.character(df$nr_seats)

    # new_used ----------------------------------------------------------------
    df$new_used <- as.character(df$new_used)

    # fuel_type ---------------------------------------------------------------
    df$fuel_type <- as.character(df$fuel_type)

    # damaged -----------------------------------------------------------------
    df$damaged <- as.character(df$damaged)

    # price_PLN ---------------------------------------------------------------
    df$price_PLN <- as.numeric(as.character(df$price_PLN))
    
    # create var age ----------------------------------------------------------
    df$age <- as.numeric(year(df$created_at_first)-df$year)
    
    # select final vars -------------------------------------------------------
    
    vars <- 
      c("ad_id", "make", "model", "mileage", "year",
        "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
        "new_used", "fuel_type", "damaged", "price_PLN", "age")
    
    df <- df[, vars, with = FALSE]
    
    gc()
    
    return(df)
 }
