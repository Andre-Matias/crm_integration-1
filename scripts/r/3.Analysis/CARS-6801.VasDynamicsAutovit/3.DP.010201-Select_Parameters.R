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
library("corrplot")
library("RcppRoll")

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

dfAds <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Ads_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAds_tmp <-
  dfAds %>%
  filter(category_id %in% c(29, 81), net_ad_counted == 1) %>%
  select(id, created_at_first) %>%
  mutate(created_at_first_day = as.Date(created_at_first),
         ad_id = as.character(id)
  ) %>%
  select(-created_at_first, -id) %>%
  filter(created_at_first_day >= as.Date('2017-10-01'))

dfTargets <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Targets_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdsParameters <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "AdsParametersWIDE_AIO.RDS"), bucket = origin_bucket_path)
  )

dfAdsParameters$ad_id <- as.character(dfAdsParameters$ad_id)

dfTargets <- dfTargets[dfTargets$ad_id %in% dfAds_tmp$ad_id, ]
dfAdsParameters <- dfAdsParameters[dfAdsParameters$ad_id %in% dfAds_tmp$ad_id, ]
dfAdsParameters <- dfAdsParameters[dfAdsParameters$price_currency == 'EUR', ]
dfAdsParameters <- dfAdsParameters[dfAdsParameters$price_gross_net == 'gross', ]

dfTargets <- dfTargets[ , c("ad_id",
                            "qtyAdImpressions_7",
                            "qtyAdPageView_7",
                            "qtyMessagesOnAtlas_7",
                            "reply_chat_sent_7", 
                            "reply_message_sent_7",
                            "reply_phone_call_7", 
                            "reply_phone_show_7",
                            "reply_phone_sms_7"
                            )
                        ]

#join dataframes

df <- 
  dfTargets %>%
  left_join(dfAdsParameters, by = c("ad_id")) %>%
  group_by() %>%
  select(-ad_id)

# make
df$make <- as.factor(df$make)

top_make <-
  df %>%
  group_by(make) %>%
  summarise(qtyByMake = sum(n())) %>% 
  mutate(perByMake = qtyByMake / sum(qtyByMake)) %>%
  group_by() %>%
  arrange(-perByMake) %>%
  mutate(cumsum_perByMake = cumsum(perByMake)) %>%
  filter(cumsum_perByMake <= 0.80) %>%
  select(make) %>%
  mutate(make = as.character(make))

df_make <- df[df$make %in% top_make$make, ]
df_make$make <- as.character(df_make$make)

df_tmp <- 


res.aov <- aov(qtyAdImpressions_7 ~ make, data = df_make)
summary(res.aov)
TukeyHSD(res.aov)

boxplot(qtyAdImpressions_7 ~ make, data = df_make)
boxplot(qtyAdPageView_7 ~ make, data = df_make, ylim = c(0, 1000))
boxplot(qtyMessagesOnAtlas_7 ~ make, data = df_make, ylim = c(0, 3))

# 
res <- cor(df_make)
corrplot(res, type = "upper", tl.col = "black", tl.srt = 45, addCoef.col = TRUE)

df_summary_stats <-
  df %>%
  group_by(nr_images) %>%
  summarise(meanMessagesOnAtlas_7 = mean(qtyMessagesOnAtlas_7)
            )%>%
  filter(!is.na(nr_images), nr_images >0)

res <- cor(df_summary_stats)
corrplot(res, type = "upper", tl.col = "black", tl.srt = 45, addCoef.col = TRUE)