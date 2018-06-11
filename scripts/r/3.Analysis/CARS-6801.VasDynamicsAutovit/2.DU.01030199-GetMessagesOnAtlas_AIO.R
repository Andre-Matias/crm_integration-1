
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

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

# list all parameters files----------------------------------------------------
s3Files_Ads <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/autovitRO/MessagesOnAtlas/RDS/")
  )

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(s3Files_Ads$Key, function (x){
    print(Sys.time())
    print(x)
    data.table(
      s3readRDS(x, bucket = bucket_path)
      )
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  as_tibble(rbindlist(dat_list, use.names = TRUE, fill = TRUE))

dat$day <- as.Date(dat$posted_date)
dat$posted_date <- NULL
dat$ad_id <- as.character(dat$ad_id)

 s3saveRDS(
   x = dat, 
   object = "datalake/autovitRO/AIO/MessagesOnAtlas_AIO.RDS",
   bucket = bucket_path
 )