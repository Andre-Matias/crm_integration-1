# load libraries --------------------------------------------------------------
library("data.table")
library("feather")
library("stringr")
library("ggplot2")
library("tidyr")
library("dplyr")
library("dtplyr")
library("magrittr")

fExtract <-
  function(dfFunction){
# find and extract params names -----------------------------------------------

a <- unique(unlist(str_extract_all(dfFunction$params, "\\<br\\>([a-z].*?)<=>")))
b <- unique(unlist(str_extract_all(dfFunction$params, "^([a-z].*?)<=>")))
c <- unique(gsub("<=>", "", gsub("<br>", "", c(a,b))))

c <- gsub("\\[", "\\\\[", c)
c <- gsub("\\]", "\\\\]", c)

# select only id and params ---------------------------------------------------
dfFunctionParams <- dfFunction[, c("id", "params")]

# extract value for each param ------------------------------------------------
dfAll_params <- data.frame()
dfTmp <- data.frame()

for(paramName in c){
  
  print (paramName)
  
  paramValue <- 
    str_extract(dfFunctionParams$params, paste0(paramName, "<=>(.*?)\\<br\\>"))
  
  paramValue <- 
    gsub(paramName, "", paramValue)
  
  paramValue <- 
    gsub("<=>", "", paramValue)
  
  paramValue <- 
    gsub("<br>", "", paramValue)
  
  dfTemp <- as.data.frame(cbind(dfFunctionParams$id, paramValue))
  dfTemp$paramName <- paramName
  
  if (nrow(dfAll_params)==0){
    dfAll_params <- dfTemp
  } else {
    dfAll_params <- rbind (dfAll_params, dfTemp)
  }
}

rm(list = c("dfTemp", "dfTmp"))
gc()

# select features -------------------------------------------------------------
dfAll_features <- 
  dfAll_params %>%
  filter(paramName == "features") %>%
  mutate(paramValue = strsplit(as.character(paramValue), "<->")) %>% 
  unnest(paramValue)
gc()



# remove objects --------------------------------------------------------------

rm(list = c("a", "b", "c", "paramName", "paramValue", "dfFunctionParams"))
gc()

# remove features from data.frame ---------------------------------------------

dfAll_params <-
  dfAll_params %>%
  filter(paramName != "features")


# extract price ---------------------------------------------------------------
dfFunction$priceValue <- 
  str_extract(dfFunction$params, paste0("price", "<=>([0-9].*?)\\<br\\>"))

dfFunction$priceValue <- 
  gsub("price<=>", "", dfFunction$priceValue)

dfFunction$priceValue <-
  gsub("<br>", "", dfFunction$priceValue)

# remove columns --------------------------------------------------------------
dfFunction <-
  dfFunction %>%
  select(id, region_id, category_id, subregion_id, district_id, city_id, 
         user_id, created_at_first, private_business, net_ad_counted, 
         new_used, priceValue)
gc()

# -----------------------------------------------------------------------------

dfAll_params <- dfAll_params[ , c("V1", "paramName", "paramValue")]
dfAll_features <- dfAll_features[ , c("V1", "paramValue")]

names(dfAll_features) <- c("V1", "paramName")

dfAll_features$paramValue <- TRUE

names(dfAll_features) <- c("ad_id", "paramName", "paramValue")
names(dfAll_params) <- c("ad_id", "paramName", "paramValue")

# features wide

dfAll_features_wide <-
  dfAll_features %>%
  group_by(ad_id, paramName) %>%
  summarise(paramValue = max(paramValue)) %>%
  spread(key = paramName, value = paramValue)

# ----

dfAll_params_wide <-
  dfAll_params %>%
  group_by(ad_id, paramName, paramValue) %>%
  summarise() %>%
  spread(key = paramName, value = paramValue)

newList <- list("features" = dfAll_features_wide,
                "params" = dfAll_params_wide,
                "all" = dfFunction
                )
return(newList)
}

