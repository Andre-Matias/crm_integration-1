# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")
library("data.table")
library("dplyr")
library("dtplyr")
library("tidyr")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "otomotoPL"
file <- "adsparameters"

startDate <- "2017-10-01"
endDate <- as.character(Sys.Date()-1)

AllDates <- seq.Date(as.Date(startDate), as.Date(endDate), 1)

# getting file list -----------------------------------------------------------
s3Files_Ads <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/ads/RDS/")
    )

# getting file list -----------------------------------------------------------
s3Files_Ads_Parameters <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/adsparameters/RDS/")
  )

# check existing files --------------------------------------------------------

listExistingDates_Ads <-
  as.Date(str_extract(s3Files_Ads$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

listExistingDates_Ads_Parameters <-
  as.Date(str_extract(s3Files_Ads_Parameters$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------

filesToTransform <- 
  s3Files_Ads$Key[!(listExistingDates_Ads %in% listExistingDates_Ads_Parameters)]

for(file in filesToTransform){
  
  dfTmp <- NULL
  
  fileParameters <- gsub("_ads_", replacement = "_adsparameters_", file)
  fileParameters <- gsub("/ads/", replacement = "/adsparameters/", file)
  
  print(paste0(Sys.time(), " => Starting Ads Parameters Transformation"))
  
  print(paste0("Reading: ", bucket_path, file))
  
  dfTmp <- s3readRDS(file, bucket = bucket_path)
  dfTmp <- as_tibble(dfTmp[, c("id", "params")])
  colnames(dfTmp) <- c("ad_id", "params")
  
  t0 <-
    dfTmp %>%
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
  
  dfParams <- 
    t0 %>% 
    filter(paramName != "features") %>%
    group_by(ad_id, paramName) %>%
    summarise(paramValue = max(paramValue)) 
  
  print(paste0("Writing: ", bucket_path, fileParameters))
  
  s3saveRDS(
    x = dfParams,
    bucket = bucket_path,
    object =  fileParameters
  )
}