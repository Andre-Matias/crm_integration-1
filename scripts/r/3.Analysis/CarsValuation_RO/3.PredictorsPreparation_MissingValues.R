#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")


fMissingValues <-
  function(df){
    # select variables --------------------------------------------------------
    
    # vars <- 
    # c("ad_id", "make", "model", "mileage", "year", 
    #   "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    #   "new_used", "fuel_type", "damaged", "price_PLN", "age")
    # 
    # df <- df[, vars, with = FALSE]
    
    # make --------------------------------------------------------------------
    df[(nchar(df$make) < 1), c("make")] <- NA
    
    # model -------------------------------------------------------------------
    df[(nchar(df$model) < 1), c("model")] <- NA
    
    # mileage -----------------------------------------------------------------

    # year --------------------------------------------------------------------

    # body_type ---------------------------------------------------------------
    df[(nchar(df$body_type) < 1), c("body_type")] <- NA
    
    # engine_capacity ---------------------------------------------------------

    # engine_power ------------------------------------------------------------

    # gearbox -----------------------------------------------------------------
    df[(nchar(df$gearbox) < 1), c("gearbox")] <- NA
    
    # nr_seats ----------------------------------------------------------------
    #df[(nchar(df$nr_seats) < 1), c("nr_seats")] <- NA
    
    # new_used ----------------------------------------------------------------
    #df[(nchar(df$new_used) < 1), c("new_used")] <- NA
    
    # fuel_type ---------------------------------------------------------------
    df[(nchar(df$fuel_type) < 1), c("fuel_type")] <- NA
    
    # damaged -----------------------------------------------------------------
    df[(nchar(df$damaged) < 1), c("damaged")] <- NA
    
    # price_PLN ---------------------------------------------------------------

    # select final vars -------------------------------------------------------
    
    
    
    # only observations without missings values -------------------------------
    df <- df[complete.cases(df), vars, with = FALSE]   # will end up from 807446 rows (fron 1Jan 2017) to 219175 rows of data
    
    gc()
    
    return(df)
    
    }
