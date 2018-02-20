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
library("tidyr")

options(scipen = 9999)

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

# custom function -------------------------------------------------------------

s3FeatherToRDS <- 
  function(x,y,z){
    oldname <- x
    print(oldname)
    newname <- gsub(y,z,x)
    newname <- gsub("feather","RDS", newname)
    print(newname)
    tmp <-
      s3read_using(
      FUN = read_feather, object = x, bucket = s3BucketName
    )
    s3saveRDS(x = tmp, object = newname, bucket = s3BucketName)
}

# read feather files ----------------------------------------------------------

file.list <- as.data.frame(
  get_bucket(bucket = s3BucketName,
             prefix = "daniel.rocha/RIX/RAW/ads/RDL_OtomotoPL_ads_")
)

# save feather as RDS ---------------------------------------------------------
lapply(file.list$Key,
       function(x) s3FeatherToRDS(x, "/ads/", "/ads/RDS/"))

# read RDS files --------------------------------------------------------------
file.list.RDS <- as.data.frame(
  get_bucket(
    bucket = s3BucketName,
    prefix = "daniel.rocha/RIX/RAW/ads/RDS/")
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
          object = "CARS-5165/dfAdsAIO.RDS",
          bucket = s3BucketName)

dat29 <-
  dat %>%
  filter(created_at_first >= '2017-01-01 00:00:00',
         created_at_first < '2017-07-01 00:00:00',
         category_id == 29
         )

# save active ads df ----------------------------------------------------------
s3saveRDS(x = dat29,
          object = "CARS-5165/dfAds_cat29_AIO.RDS",
          bucket = s3BucketName)

# save active ads df ----------------------------------------------------------
s3saveRDS(x = dat29[, c("id")],
          object = "CARS-5165/dfAds_cat29_id_list.RDS",
          bucket = s3BucketName)

# load ads df -----------------------------------------------------------------
dat29 <-
  s3readRDS(
          object = "CARS-5165/dfAds_cat29_AIO.RDS",
          bucket = s3BucketName)

t0 <-
  dat29 %>%
  unnest(params = strsplit(params, "<br>")) %>%
  mutate(new = strsplit(params, "<=>"),
         length = lapply(new, length)) %>%
  filter(length >=2 ) %>%
  mutate(paramName = unlist(lapply(new, function(x) x[1])),
         paramValue = unlist(lapply(new, function(x) x[2]))
  ) %>%
  select(ad_id, paramName, paramValue)

t0[t0$paramName=="price" & !is.na(as.numeric(t0$paramValue)), c("paramName")] <- "priceValue"

t0$paramName <- gsub("price\\[currency\\]", "price_currency", t0$paramName, perl = TRUE)
t0$paramName <- gsub("price\\[gross_net\\]", "price_gross_net", t0$paramName, perl = TRUE)

print(paste(as.POSIXct(Sys.time()), as.POSIXct(Sys.time()) - rt0, 
            "params to wide"))
dfParams <- 
  t0 %>% 
  filter(paramName != "features") %>%
  group_by(ad_id, paramName) %>%
  summarise(paramValue = max(paramValue)) %>%
  spread(key = paramName, value = paramValue)

gc()

