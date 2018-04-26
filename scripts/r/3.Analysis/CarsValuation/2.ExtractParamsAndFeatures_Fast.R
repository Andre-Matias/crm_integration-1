library("feather")
library("stringr")
library("data.table")
library("dplyr")
library("tidyr")
library("dtplyr")


# Function to extract parameters ------------------------------------------

fExtract <-
  function(t){

    rt0 <- as.POSIXct(Sys.time())
    print(paste(as.POSIXct(Sys.time()), "start"))
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0,
                "starting extraction"))
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0,
                "selecting ad_id, params"))

    t <- as_tibble(t[, c("id", "params")])
    
    gc()
  
    colnames(t) <- c("ad_id", "params")
    
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0,
                "extracting from params field"))
    
    # tidy dataset format and clean parameters ----------------------------
    t0 <-
      t %>%
      unnest(params = strsplit(params, "<br>")) %>%
      mutate(new = strsplit(params, "<=>"),
             length = lapply(new, length)) %>%
      filter(length >=2 ) %>%
      mutate(paramName = unlist(lapply(new, function(x) x[1])),
             paramValue = unlist(lapply(new, function(x) x[2]))
      ) %>%
      select(ad_id, paramName, paramValue)
    
    t0[t0$paramName=="price" & !is.na(as.numeric(t0$paramValue)), c("paramName")] <- "priceValue"
    
    t0$paramName <- gsub("price\\[currency\\]", "price_currency", t0$paramName, perl = TRUE)
    t0$paramName <- gsub("price\\[gross_net\\]", "price_gross_net", t0$paramName, perl = TRUE)

    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
                "params to wide"))
    

    # create a data frame for parameters --------------------------------------
    dfParams <- 
      t0 %>% 
      filter(paramName != "features") %>%
      group_by(ad_id, paramName) %>%
      summarise(paramValue = max(paramValue)) %>%
      spread(key = paramName, value = paramValue)
    
    gc()
    
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
                "features to wide"))
    
    # create a data frame for features     ------------------------------------  
    dfFeatures <-
      t0 %>% 
      filter(paramName == "features") %>%
      unnest(paramName = strsplit(paramValue, "<->")) %>%
      select(ad_id, paramName) %>%
      group_by(ad_id, paramName) %>%
      summarise(paramValue = sum(n())) %>%
      mutate(paramValue = "Yes") %>%
      spread(key = paramName, value = paramValue)
    
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
                "getting price"))
          
    # extract price -----------------------------------------------------------
    t$priceValue <- 
      str_extract(t$params, paste0("price", "<=>([0-9].*?)\\<br\\>"))
    
    t$priceValue <- 
      gsub("price<=>", "", t$priceValue)
    
    t$priceValue <-
      gsub("<br>", "", t$priceValue)
    
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
                "generating list"))
    
    newList <- list("features" = dfFeatures,
                    "params" = dfParams
    )
    
    print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
                "returning list"))
    
    return(newList)
    
    print("end")
    
  }





