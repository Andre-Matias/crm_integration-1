# load libraries --------------------------------------------------------------
library("aws.s3")
library("magrittr")
library("dplyr")
library("data.table")
library("dtplyr")
library("readr")
library("stringr")
library("tidyr")
library("ggplot2")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

vertical <- "autovitRO"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#remove trash 
rm(list = setdiff(ls(), c("myS3key", "MyS3SecretAccessKey")))

origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"

dfTargets <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Targets_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdsImages <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_adsimages_AIO.RDS"), bucket = origin_bucket_path)
  )

#join dataframes

df <- 
  dfTargets %>%
  left_join(dfAdsImages, by = c("ad_id")) %>%
  select(-ad_id)

