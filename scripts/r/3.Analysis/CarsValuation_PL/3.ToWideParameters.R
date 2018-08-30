# load libraries --------------------------------------------------------------
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("tidyr")


# Join features, params and other ads info in one data frame --------------

fAllInOne <-
  function(dfFeaturesWide, dfParamsWide, dfMainAds){
  dfAll_features_wide <- dfFeaturesWide
  dfAll_params_wide <- dfParamsWide
  dfCarsAds <- dfMainAds

  # clean variables  
  colnames(dfAll_params_wide) <- gsub("\\\\\\[", "_", colnames(dfAll_params_wide))
  colnames(dfAll_params_wide) <-gsub("\\\\\\]", "_", colnames(dfAll_params_wide))
  
  dfAll_features_wide$ad_id <- as.numeric(as.character(dfAll_features_wide$ad_id))
  dfAll_params_wide$ad_id <- as.numeric(as.character(dfAll_params_wide$ad_id))
  dfCarsAds$ad_id <- as.numeric(as.character(dfCarsAds$id))
  
  dfAll_features_wide <- as.data.table(dfAll_features_wide)
  dfAll_params_wide <- as.data.table(dfAll_params_wide)
  dfCarsAds <- as.data.table(dfCarsAds)

  dfCarsAdsWide <- 
    dfCarsAds %>%
    left_join(dfAll_params_wide, by = c("ad_id"="ad_id")) %>%
    left_join(dfAll_features_wide, by = c("ad_id"="ad_id"))
  
  dfCarsAdsWide[, id:=NULL]

  # only live ads ---------------------------------------------------------------
  dfCarsAdsWide <- 
    dfCarsAdsWide[dfCarsAdsWide$net_ad_counted == "1", ]
  return(dfCarsAdsWide)
  }