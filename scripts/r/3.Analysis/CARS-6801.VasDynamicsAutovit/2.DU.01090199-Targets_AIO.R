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

dfAds <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Ads_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdsImpressions <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, vertical, "_adsimpressions_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdPage <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, vertical, "_adpage_AIO.RDS"), bucket = origin_bucket_path)
  )

dfReplies <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, vertical, "_replies_AIO.RDS"), bucket = origin_bucket_path)
  )

dfMessagesOnAtlas <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "MessagesOnAtlas_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdsImpressions <- dfAdsImpressions[dfAdsImpressions$ad_id %in% dfAds$ad_id, ]
dfAdPage <- dfAdPage[dfAdPage$ad_id %in% dfAds$ad_id, ]
dfReplies <- dfReplies[dfReplies$ad_id %in% dfAds$ad_id, ]
dfMessagesOnAtlas <- dfMessagesOnAtlas[dfMessagesOnAtlas$ad_id %in% dfAds$ad_id, ]

dfTmp <-
  dfAds %>%
  full_join(dfAdsImpressions, by = c("ad_id"))
gc()

dfTmp <-
  dfTmp %>%
  full_join(dfAdPage, by = c("ad_id", "day"))
gc()

dfTmp <-
  dfTmp %>%
  full_join(dfReplies, by = c("ad_id", "day"))
gc()

dfTmp <-
  dfTmp %>%
  full_join(dfMessagesOnAtlas, by = c("ad_id", "day"))
gc()

dfTmp <- dfTmp[!is.na(dfTmp$ad_id), ]

dfTmp <- 
  dfTmp %>%
  mutate(difftime = difftime(day, dfTmp$created_at_first_day, units = "day"))

dfTmp$difftime <- 0
dfTmp$difftime_cut[dfTmp$difftime < 7] <- 7
dfTmp$difftime_cut[dfTmp$difftime < 14 & dfTmp$difftime >= 7] <- 14
dfTmp$difftime_cut[dfTmp$difftime < 21 & dfTmp$difftime >= 14] <- 21
dfTmp$difftime_cut[dfTmp$difftime < 28 & dfTmp$difftime >= 21] <- 28
dfTmp <- dfTmp[dfTmp$difftime < 28, ]

dfTmp1 <-
  dfTmp %>%
  gather(key = metric, value = value, -created_at_first_day, -ad_id, -day, -difftime, -difftime_cut) %>%
  filter(!is.na(ad_id)) %>%
  mutate(metric = paste0(metric, "_", difftime_cut)) %>%
  group_by(ad_id, metric) %>%
  summarise(value = sum(value, na.rm = TRUE)) %>%
  spread(key = metric, value = value, fill = 0)

s3saveRDS(x = dfTmp1,
          object = paste0(origin_bucket_prefix, "Targets_AIO.RDS"), 
          bucket = origin_bucket_path
)