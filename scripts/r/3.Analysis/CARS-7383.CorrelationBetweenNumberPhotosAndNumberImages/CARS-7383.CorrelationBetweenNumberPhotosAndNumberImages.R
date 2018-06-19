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
library("tidy")
library("ggthemes")

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

dfRaw <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "dfInputToModel_AQS.RDS"), bucket = origin_bucket_path)
  )

dfRaw <- dfRaw[dfRaw$nr_images != 0, ]

res <- cor(dfRaw[, c("nr_images", "qtyAdImpressions_7", "qtyAdPageView_7", "qtyMessagesOnAtlas_7", "reply_phone_show_7", "reply_phone_call_7")])
corrplot(res, type = "upper",
         tl.col = "black", tl.srt = 45, addCoef.col = TRUE)


# correlation between the mean of metrics by group # should not be done

dfStats <-
  dfRaw %>%
  group_by()%>%
  select(nr_images, qtyAdImpressions_7, qtyAdPageView_7, qtyMessagesOnAtlas_7, reply_phone_show_7, reply_phone_call_7) %>%
  gather(key = "event", value = "value", contains("7")) %>%
  group_by(nr_images, event) %>%
  summarise(sumEvents = sum(value),
            countAds = sum(n())
            ) %>%
  mutate(mean = sumEvents / countAds) %>%
  select(-sumEvents, -countAds) %>%
  spread(key = event, value = mean) %>%
  group_by() %>%
  arrange(nr_images) %>%
  mutate_at(vars(qtyAdImpressions_7, qtyAdPageView_7, qtyMessagesOnAtlas_7), funs(chg = ((.-lag(.))/lag(.))))

res <- cor(dfStats[, c("nr_images", "qtyAdImpressions_7", "qtyAdPageView_7", "qtyMessagesOnAtlas_7", "reply_phone_show_7", "reply_phone_call_7")])
corrplot(res, type = "upper",
         tl.col = "black", tl.srt = 45, addCoef.col = TRUE)


ggplot(dfStats)+
  geom_line(aes(nr_images, qtyAdImpressions_7))+
  geom_smooth(aes(nr_images, qtyAdImpressions_7))+
  ggtitle("Mean of Ad Impressions vs Quantity of Images", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()

ggplot(dfStats)+
  geom_point(aes(nr_images, qtyAdImpressions_7_chg))+
  geom_smooth(aes(nr_images, qtyAdImpressions_7_chg))+
  ggtitle("Gain of Ad  Impressions per Aditional Image", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()


ggplot(dfStats)+
  geom_line(aes(nr_images, qtyAdPageView_7))+
  geom_smooth(aes(nr_images, qtyAdPageView_7))+
  ggtitle("Mean of Ad Page Views vs Quantity of Images", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()

ggplot(dfStats)+
  geom_point(aes(nr_images, qtyAdPageView_7_chg))+
  geom_smooth(aes(nr_images, qtyAdPageView_7_chg))+
  ggtitle("Gain of Ad  Page Views per Aditional Image", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()

ggplot(dfStats)+
  geom_line(aes(nr_images, qtyMessagesOnAtlas_7))+
  geom_smooth(aes(nr_images, qtyMessagesOnAtlas_7))+
  ggtitle("Mean of Received Messages vs Quantity of Images", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()

ggplot(dfStats)+
  geom_point(aes(nr_images, qtyMessagesOnAtlas_7_chg))+
  geom_smooth(aes(nr_images, qtyMessagesOnAtlas_7_chg))+
  ggtitle("Gain of Messages per Aditional Image", "Autovit: 01/10/17 => 30/04/2018 | 109.248 listings")+
  theme_gdocs()
