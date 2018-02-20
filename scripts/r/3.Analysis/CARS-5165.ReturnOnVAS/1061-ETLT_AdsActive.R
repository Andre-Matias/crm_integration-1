#load libraries
library("aws.s3")
library("feather")
library("dplyr")
library("data.table")
library("dtplyr")
library("fasttime")
library("magrittr")
library("ggplot2")
library("parallel")

options(scipen = 9999)

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

# read RDS files --------------------------------------------------------------
file.list.RDS <- as.data.frame(
  get_bucket(
    bucket = s3BucketName,
    prefix = "CARS-5165/DailyActiveAds/Active_")
)

# join all files in one list --------------------------------------------------
dat_list <-
  lapply(file.list.RDS$Key, function(x) s3readRDS(x, bucket = s3BucketName)
  )

# list to one dataframe -------------------------------------------------------
dat <- 
  as_tibble(rbindlist(dat_list, fill = TRUE))

# save active ads df ----------------------------------------------------------
s3saveRDS(x = dat,
          object = "CARS-5165/dfAdsActiveAIO.RDS",
          bucket = s3BucketName)

dat <-
  s3readRDS(object = "CARS-5165/dfAdsActiveAIO.RDS", bucket = s3BucketName)

fActiveTime <- function(x){
  s1 <- x["changed_at"]
  s2 <- x["start"]
  e1 <- x["next.changed_at"]
  e2 <- x["end"]
  y <- difftime(min(c(e1,e2)), max(c(s1, s2)), units = "days")
  return(y)
} 

dat$ActiveTime <- apply(dat, 1, function(x) fActiveTime(x))

s3saveRDS(x = dat,
          object = "CARS-5165/dfAdsActiveAIO_withTime.RDS",
          bucket = s3BucketName)

dfActimeTimeByDayAdID <- 
  dat %>%
  group_by(id, start) %>%
  summarise(qtyTimeLiveInDays = sum(ActiveTime))


s3saveRDS(x = dfActimeTimeByDayAdID,
          object = "CARS-5165/ActiveTimeByDayAdId.RDS",
          bucket = s3BucketName)


dfActimeTimeByDay<- 
  dat %>%
  group_by(id) %>%
  summarise(qtyTimeLiveInDays = sum(ActiveTime))


s3saveRDS(x = dfActimeTimeByDay,
          object = "CARS-5165/ActiveTimeByDay.RDS",
          bucket = s3BucketName)
