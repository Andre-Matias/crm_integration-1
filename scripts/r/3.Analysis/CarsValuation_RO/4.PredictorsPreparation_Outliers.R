#libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")


fOutliers <-
  function(df){
    # # select variables --------------------------------------------------------
    # 
    # vars <- 
    # c("ad_id", "make", "model", "mileage", "year", 
    #   "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
    #   "new_used", "fuel_type", "damaged", "price_PLN", "age")
    # 
    # df <- df[, vars, with = FALSE]
    
    df <-
      df[df$damaged == "0", ]
    
    # make --------------------------------------------------------------------

    # model -------------------------------------------------------------------

    # mileage -----------------------------------------------------------------
    dfTmp <-
      df %>%
      group_by(make) %>%
      summarise(LowerValue = quantile(mileage,probs = c(0.05), na.rm = TRUE),
                UpperValue = quantile(mileage,probs = c(0.95), na.rm = TRUE),
                qty = sum(!is.na(ad_id))
      )%>%
      inner_join(df,
                 by = c("make")) %>%
      mutate(outlier = ifelse(
        mileage <= LowerValue | mileage >= UpperValue, TRUE, FALSE)
      ) %>%
      filter(outlier == TRUE) %>%
      select(ad_id)
    
    df <-
      df[!(df$ad_id %in% dfTmp$ad_id), ]
    
    df <-
      df[df$mileage >= 5000 & df$mileage <= 300000, ]
    
    

    # year --------------------------------------------------------------------
    df$year <-
      as.numeric(as.character(df$year))
    
    df <-
      df[df$year >= 1990
         & df$year <= year(Sys.Date()), ]

    # body_type ---------------------------------------------------------------

    # engine_capacity ---------------------------------------------------------
    dfTmp <-
      df %>%
      group_by(make, model) %>%
      summarise(LowerValue = round(quantile(engine_capacity,probs = c(0.005), na.rm = TRUE),0),
                UpperValue = round(quantile(engine_capacity,probs = c(0.999), na.rm = TRUE),0),
                qty = sum(!is.na(ad_id))
      )%>%
      inner_join(df,
                 by = c("make", "model")) %>%
      mutate(outlier = ifelse(
        engine_capacity < LowerValue | engine_capacity > UpperValue, TRUE, FALSE)
      ) %>%
      filter(engine_capacity > 500 & engine_capacity < 7000) %>%
      filter(outlier == TRUE) %>%
      select(ad_id, make, model, engine_capacity, LowerValue, UpperValue)
    
    df <-
      df[!(df$ad_id %in% dfTmp$ad_id), ]
    # engine_power ------------------------------------------------------------
    
    dfTmp <-
      df %>%
      group_by(make, model) %>%
      summarise(LowerValue = quantile(engine_power,probs = c(0.01), na.rm = TRUE),
                UpperValue = quantile(engine_power,probs = c(0.995), na.rm = TRUE),
                qty = sum(!is.na(ad_id))
      )%>%
      inner_join(df,
                 by = c("make", "model")) %>%
      mutate(outlier = ifelse(
        engine_power < LowerValue | engine_power > UpperValue, TRUE, FALSE)
      ) %>%
      filter(outlier == TRUE) %>%
      select(ad_id, model, engine_power, LowerValue, UpperValue)
    
    df <-
      df[!(df$ad_id %in% dfTmp$ad_id), ]
    

    # gearbox -----------------------------------------------------------------
    df <- 
      df[!(df$gearbox==""), ]
    
    # nr_seats ----------------------------------------------------------------
    df <-
      df[df$nr_seats!="", ]

    # new_used ----------------------------------------------------------------

    # fuel_type ---------------------------------------------------------------

    # damaged -----------------------------------------------------------------

    # price_PLN ---------------------------------------------------------------
    
    # get outliers values for price -----------------------------------------------
    dfTmp <-
      df %>%
      group_by(make, model) %>%
      summarise(LowerValue = quantile(price_PLN,probs = c(0.05), na.rm = TRUE),
                UpperValue = quantile(price_PLN,probs = c(0.95), na.rm = TRUE),
                qty = sum(!is.na(ad_id))
      )%>%
      inner_join(df,
                 by = c("make", "model")) %>%
      mutate(outlier = ifelse(
        price_PLN <= LowerValue | price_PLN >= UpperValue, TRUE, FALSE)
      ) %>%
      filter(outlier == TRUE) %>%
      select(ad_id)
    
    df <-
      df[!(df$ad_id %in% dfTmp$ad_id), ]
    
    df <-
      df[df$price_PLN >= 500 & df$price_PLN <= 750000, ]
    
    

    # select final vars -------------------------------------------------------
    
    vars <- 
      c("ad_id", "make", "model", "mileage", "year",
        "body_type", "engine_capacity", "engine_power", "gearbox", "nr_seats",
        "new_used", "fuel_type", "damaged", "price_PLN", "age")
    
    # only observations without missings values -------------------------------
    df <- df[complete.cases(df), vars, with = FALSE]
    
    gc()
    
    return(df)
    
    }
