#load libraries
library("aws.s3")
library("feather")
library("dplyr")
library("data.table")
library("dtplyr")
library("fasttime")
library("magrittr")
library("ggplot2")

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

# custom function -------------------------------------------------------------

s3ReadAndPrint <- function(x){
  print(x)
  s3read_using(
    FUN = read_feather, object = x, bucket = s3BucketName
  )
}

# read buckets content --------------------------------------------------------

file.list <- as.data.frame(
  get_bucket(bucket = s3BucketName,
             prefix = "daniel.rocha/RIX/RAW/ad_page/ati_574113_ad_page")
)

dat_list <-
  lapply(file.list$Key, function(x) s3ReadAndPrint(x))

dat <- 
  as_tibble(rbindlist(dat_list, fill = TRUE ))

rm("file.list")
gc()

# ad page by day and platform -------------------------------------------------

dat <- 
  dat %>% 
  mutate(d_time_date = fastPOSIXct(d_time_date))

# AdPageByDayPlatform <-
#   dat %>%
#   filter(d_time_date >= '2017-01-01', d_time_date < '2017-07-01') %>%
#   group_by(d_time_date, cd_platfv2) %>%
#   summarise(qtyAdPageImpressions = sum(cm_32399))

# ggplot(StatsByDayPlatform)+
#   geom_line(aes(x=d_time_date, y=qtyAdPageImpressions, color = cd_platfv2))+
#   geom_smooth(method = 'loess', aes(x=d_time_date, y=qtyAdPageImpressions, color = cd_platfv2 ))
# 
# AdPageByDay <-
#   dat %>%
#   group_by(d_time_date) %>%
#   filter(d_time_date >= '2017-01-01', d_time_date < '2017-07-01') %>%
#   summarise(qtyAdPageImpressions = sum(cm_32399))

AdPageByDayAdId <-
  dat %>%
  group_by(d_time_date, cd_adidv2) %>%
  summarise(qtyAdPageLoad = sum(cm_32399)) %>%
  filter(d_time_date >= '2017-01-01', d_time_date < '2017-07-01')

s3saveRDS(x = AdPageByDayAdId,
          object = "AdPageByDayAdId.RDS",
          bucket = "pyrates-data-ocean/CARS-5165")

rm("AdPageByDayAdId")
gc()


# AdPageByDayAdIdPlatform <-
#   dat %>%
#   group_by(d_time_date, cd_adidv2, cd_platfv2) %>%
#   summarise(qtyAdPageImpressions = sum(cm_32399)) %>%
#   filter(d_time_date >= '2017-01-01', d_time_date < '2017-07-01')
# 
# s3saveRDS(x = AdPageByDayAdIdPlatform,
#           object = "AdPageByDayAdIdPlatform.RDS",
#           bucket = "pyrates-data-ocean/CARS-5165")
# 
# rm("AdPageByDayAdIdPlatform")
# gc()

# ggplot(StatsByDayPlatform)+
#   geom_line(aes(x=d_time_date, y=qtyAdPageImpressions))+
#   geom_smooth(method = 'loess', aes(x=d_time_date, y=qtyAdPageImpressions))
