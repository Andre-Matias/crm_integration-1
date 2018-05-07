
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

# parameters to consider ------------------------------------------------------
listParameters <-
  c("make", 
    "model", 
    "year", 
    "mileage", 
    "engine_power", 
    "engine_capacity", 
    "fuel_type", 
    "body_type", 
    "gearbox",
    "price_currency",
    "price_gross_net",
    "priceValue",
    "damaged"
    )

# list all parameters files----------------------------------------------------
s3Files_Ads_Parameters <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/adsparameters/RDS/")
  )

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(s3Files_Ads_Parameters$Key, function (x){
    print(Sys.time())
    print(x)
    data.table(
      s3readRDS(x, bucket = bucket_path)
      )[paramName %in% listParameters]
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

dat_wide <-
  dat %>%
  spread(key = paramName, value = paramValue)

s3saveRDS(
  x = dat_wide, 
  object = "datalake/otomotoPL/AIO/AdsParametersWIDE_AIO.RDS",
  bucket = bucket_path
)