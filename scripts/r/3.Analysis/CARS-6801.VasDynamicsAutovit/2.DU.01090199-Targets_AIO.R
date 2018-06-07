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

origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"

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

dfMessagesOnAtlas$day <- as.Date(dfMessagesOnAtlas$posted_date)
dfMessagesOnAtlas$posted_date <- NULL
dfMessagesOnAtlas$ad_id <- as.character(dfMessagesOnAtlas$ad_id)
gc()

dfAds_tmp <-
  dfAds %>%
  filter(category_id %in% c(29, 81)) %>%
  select(id, created_at_first) %>%
  mutate(created_at_first_day = as.Date(created_at_first),
         ad_id = as.character(id)
  ) %>%
  select(-created_at_first, -id) %>%
  filter(created_at_first_day >= as.Date('2017-10-01'))

gc()

dfAdsImpressions <- dfAdsImpressions[dfAdsImpressions$ad_id %in% dfAds_tmp$ad_id, ]
dfAdPage <- dfAdPage[dfAdPage$ad_id %in% dfAds_tmp$ad_id, ]
dfReplies <- dfReplies[dfReplies$ad_id %in% dfAds_tmp$ad_id, ]
dfMessagesOnAtlas <- dfMessagesOnAtlas[dfMessagesOnAtlas$ad_id %in% dfAds_tmp$ad_id, ]

dfTmp <-
  dfAds_tmp %>%
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