# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")
options(scipen = 9999)

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# getting file list -----------------------------------------------------------
s3Files_RepliesByDay <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/repliesbyday/")
  )

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(s3Files_RepliesByDay$Key, function (x){
    x <- paste0(bucket_path, x)
    print(x)
    data.table(s3readRDS(x))
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

RepliesByAdidDateEventname_long <-
  dat %>%
  group_by(ad_id, date, eventname) %>%
  summarise(qtyRepliesByDay = sum(qtyRepliesByDay, na.rm = TRUE))

s3saveRDS(
  x = RepliesByAdidDateEventname_long, 
  bucket = bucket_path, 
  object = "datalake/otomotoPL/AIO/RepliesByAdidDateEventname_long.RDS"
)

RepliesByAdidEventname_long <-
  dat %>%
  group_by(ad_id, eventname) %>%
  summarise(qtyRepliesByDay = sum(qtyRepliesByDay, na.rm = TRUE))

s3saveRDS(
  x = RepliesByAdidEventname_long, 
  bucket = bucket_path, 
  object = "datalake/otomotoPL/AIO/RepliesByAdidEventname_long.RDS"
)

RepliesByAdidDateEventname_wide <-
  dat %>%
  group_by(ad_id, date, eventname) %>%
  summarise(qtyRepliesByDay = sum(qtyRepliesByDay, na.rm = TRUE)) %>%
  spread(key = eventname, value = sum(qtyRepliesByDay, na.rm = TRUE), fill = 0)

s3saveRDS(
  x = RepliesByAdidDateEventname_wide, 
  bucket = bucket_path, 
  object = "datalake/otomotoPL/AIO/RepliesByAdidDateEventname_wide.RDS"
)

RepliesByDateEventname_long <-
  dat %>%
  group_by(date, eventname) %>%
  summarise(qtyRepliesByDay = sum(qtyRepliesByDay, na.rm = TRUE))

s3saveRDS(
  x = RepliesByDateEventname_long, 
  bucket = bucket_path, 
  object = "datalake/otomotoPL/AIO/RepliesByDateEventname_long.RDS"
)

RepliesByAdidEventname_wide <-
  dat %>%
  group_by(ad_id, eventname) %>%
  summarise(qtyRepliesByDay = sum(qtyRepliesByDay, na.rm = TRUE)) %>%
  spread(key = eventname, value = sum(qtyRepliesByDay, na.rm = TRUE), fill = 0)

s3saveRDS(
  x = RepliesByAdidEventname_wide, 
  bucket = bucket_path, 
  object = "datalake/otomotoPL/AIO/RepliesByAdidEventname_wide.RDS"
)
