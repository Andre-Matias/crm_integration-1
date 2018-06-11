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
library("lubridate")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#config
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/"
vertical <- "autovitRO"
filename <- "DescriptionLength"

startDate <- "2017-10-01"
endDate <- as.character(Sys.Date()-1)

AllDates <- seq.Date(as.Date(startDate), as.Date(endDate), 1)

# getting file list -----------------------------------------------------------
s3Files_Input <- 
  as.data.frame(
    get_bucket(
      bucket = origin_bucket_path,
      max = Inf, prefix = paste0(origin_bucket_prefix, "ads/RDS/"))
    )

# getting file list -----------------------------------------------------------
s3Files_Output <- 
  as.data.frame(
    get_bucket(
      bucket = origin_bucket_path,
      max = Inf, prefix = paste0(origin_bucket_prefix, filename, "/"))
  )

# check existing files --------------------------------------------------------

listExistingDates_Input <-
  as.Date(str_extract(s3Files_Input$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

listExisting_Output <-
  as.Date(str_extract(s3Files_Output$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------

filesToTransform <- 
  s3Files_Input$Key[!(listExistingDates_Input %in% listExisting_Output)]

for(file in filesToTransform){
  
  dfTmp <- NULL
  dfTmp <- s3readRDS(file, bucket = origin_bucket_path)
  
  dfTmp <- 
    dfTmp %>%
    select(id, description) %>%
    filter(!is.na(description)) %>%
    mutate(
      DescriptionLength = as.numeric(nchar(iconv(description, to = "UTF-8"))),
      ad_id = as.character(id)
    ) %>%
    select(ad_id, DescriptionLength) %>%
    as_tibble()
  
  
  file <- gsub("ads", filename, file)
  print(paste0("Writing: ", origin_bucket_path, file))

   s3saveRDS(
     x = dfTmp,
     bucket = origin_bucket_path,
     object =  file
   )
}