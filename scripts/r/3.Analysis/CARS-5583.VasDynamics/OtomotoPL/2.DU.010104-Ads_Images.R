# load libraries --------------------------------------------------------------
library("aws.s3")
library("dplyr")
library("magrittr")
library("dtplyr")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")
library("stringr")
library("DescTools")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "otomotoPL"
file <- "adsimages"

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
      max = Inf, prefix = "datalake/otomotoPL/adsimages/RDS/")
  )

# check existing files --------------------------------------------------------

listExistingDates_Ads <-
  as.Date(str_extract(s3Files_Ads$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

listExistingDates_Ads_Parameters <-
  as.Date(str_extract(s3Files_Ads_Parameters$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# files to transform

# dates to get ----------------------------------------------------------------

filesToTransform <- 
  s3Files_Ads$Key[!(listExistingDates_Ads %in% listExistingDates_Ads_Parameters)]

for(file in filesToTransform){
  
  dfTmp <- NULL
  
  fileParameters <- gsub("ads", replacement = "adsimages", file)
  
  print(paste0(Sys.time(), " => Starting Ads Parameters Transformation"))
  
  print(paste0("Reading: ", bucket_path, file))
  
  dfTmp <- s3readRDS(file, bucket = bucket_path)
  dfTmp <- as_tibble(dfTmp[, c("id", "riak_mapping")])
  colnames(dfTmp) <- c("ad_id", "riak_mapping")
  
  
  dfTmp$nr_images <- lapply(dfTmp$riak_mapping, function(x) sum(as.binary(x)))
  
  dfTmp <- dfTmp[, c("ad_id", "nr_images")]
  
  print(paste0("Writing: ", bucket_path, fileParameters))
  
  s3saveRDS(
    x = dfTmp,
    bucket = bucket_path,
    object =  fileParameters
  )
}