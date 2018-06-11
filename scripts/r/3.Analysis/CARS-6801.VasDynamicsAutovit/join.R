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

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#config
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"
vertical <- "autovitRO"

dfTargets <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Targets_AIO.RDS"), bucket = origin_bucket_path)
  )

dfNrImages <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_adsimages_AIO.RDS"), bucket = origin_bucket_path)
  )

dfNrImages <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_adsimages_AIO.RDS"), bucket = origin_bucket_path)
  )

dfParameters <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "AdsParametersWIDE_AIO.RDS"), bucket = origin_bucket_path)
  )

dfParameters$ad_id <- as.character(dfParameters$ad_id)

dfVAS <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "VAS_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAds_DateTime <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_datetime_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAds_DescriptionLength <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_DescriptionLength_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAds_PrivateBusiness <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "autovitRO_PrivateBusiness_AIO.RDS"), bucket = origin_bucket_path)
  )


dfFinal <-
  dfTargets %>%
  left_join(dfNrImages, by = c("ad_id")) %>%
  left_join(dfParameters, by = c("ad_id")) %>%
  left_join(dfVAS, by = c("ad_id")) %>%
  left_join(dfAds_DateTime, by = c("ad_id")) %>%
  left_join(dfAds_DescriptionLength, by = c("ad_id")) %>%
  left_join(dfAds_PrivateBusiness, by = c("ad_id"))
  

dfFinal$ad_bighomepage[is.na(dfFinal$ad_bighomepage)] <- 0
dfFinal$ad_homepage[is.na(dfFinal$ad_homepage)] <- 0
dfFinal$bump_up[is.na(dfFinal$bump_up)] <- 0
dfFinal$export_olx[is.na(dfFinal$export_olx)] <- 0
dfFinal$highlight[is.na(dfFinal$highlight)] <- 0
dfFinal$topads[is.na(dfFinal$topads)] <- 0

s3saveRDS(x = dfFinal,
          object = paste0(origin_bucket_prefix, "dfInputToModel_AQS.RDS"), 
          bucket = origin_bucket_path
)