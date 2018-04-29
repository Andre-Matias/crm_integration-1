# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# getting file list -----------------------------------------------------------
s3Files_Replies <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/replies/")
    )

# getting file list -----------------------------------------------------------
s3Files_RepliesByDay <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/repliesbyday/")
  )

# check existing files --------------------------------------------------------

listExistingDates_Replies <-
  as.Date(str_extract(s3Files_Replies$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

listExistingDates_RepliesByDay <-
  as.Date(str_extract(s3Files_RepliesByDay$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------
filesToTransform <- 
  s3Files_Replies$Key[!(listExistingDates_Replies %in% listExistingDates_RepliesByDay)]

for(file in filesToTransform){
  
  dfTmp <- NULL
  dfTmpStats <- NULL
  
  fileReplies <- gsub("replies", replacement = "repliesbyday", file)

  print(paste0(Sys.time(), " => Starting Transformation"))
  
  print(paste0("Reading: ", bucket_path, file))
  
  dfTmp <- 
    s3readRDS(file, bucket = bucket_path)
  
  dfTmpStats <-
    dfTmp %>%
    mutate(date = as.Date(as.POSIXct(server_date_trunc, origin="1970-01-01"))) %>%
    group_by(item_id, date, eventname) %>%
    summarise(qtyRepliesByDay = sum(n())) %>%
    mutate(ad_id = item_id) %>%
    group_by() %>%
    select(ad_id, date, eventname, qtyRepliesByDay)
  
  print(paste0("Writing: ", bucket_path, fileReplies))
  
  s3saveRDS(
    x = dfTmpStats,
    bucket = bucket_path,
    object =  fileReplies
  )
}