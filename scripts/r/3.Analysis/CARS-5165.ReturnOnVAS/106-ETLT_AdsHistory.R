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
             prefix = "daniel.rocha/RIX/RAW/ads_history/RDL_OtomotoPL_ads_his")
)

# save feather as RDS ---------------------------------------------------------
lapply(file.list$Key,
       function(x) s3FeatherToRDS(x, "/ads_history/", "/ads_history/RDS/"))

# read RDS files --------------------------------------------------------------
file.list.RDS <- as.data.frame(
  get_bucket(
    bucket = s3BucketName,
    prefix = "daniel.rocha/RIX/RAW/ads_history/RDS/")
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
          object = "CARS-5165/dfAdsHistoryAIO.RDS",
          bucket = s3BucketName)

# read active ads df ---------------------------------------------------------
dat <- 
  s3readRDS(object = "CARS-5165/dfAdsHistoryAIO.RDS",
            bucket = s3BucketName)

# read only cars and and created at first 2017-01-01-01 and 2017-06-30 -------
list29 <- 
  s3readRDS(object = "CARS-5165/dfAds_cat29_id_list.RDS",
            bucket = s3BucketName)

dat <- dat[dat$id %in% list29$id, ]

df <- arrange(dat, id, primary)

# make intervals
df <- 
  df %>%
  filter(!is.na(status))%>%
  group_by(id) %>%
  select(-params) %>%
  mutate(
    next.id = lead(id, order_by=primary)
    ,next.primary = lead(primary, order_by=primary)
    ,next.changed_at = lead(changed_at, order_by=primary)
    ,next.status = lead(status, order_by=primary)
  )

# save active ads df ----------------------------------------------------------
s3saveRDS(x = df,
          object = "CARS-5165/dfAdsHistoryNextStatusAIO.RDS",
          bucket = s3BucketName)

# read active ads df ----------------------------------------------------------
dfActive <- 
  as_tibble(
    s3readRDS(
      object = "CARS-5165/dfAdsHistoryNextStatusAIO.RDS",
      bucket = s3BucketName)
  )

# only active intervals -------------------------------------------------------
dfActive <- dfActive[dfActive$status=='active', ]

daily <- seq(from = as.POSIXct('2017-01-01 00:00:00'), 
             to = as.POSIXct('2017-09-30 00:00:00'), by='day')

fDay <- 
  function(x){
    start <- as.POSIXct(x)
    end <- as.POSIXct(start + as.difftime(1, unit="days"))
    print(paste(start, "=>", end))
    dfActive$start <- start
    dfActive$end <- end
    dfActive$overlap <- !(dfActive$next.changed_at <= dfActive$start | dfActive$changed_at > dfActive$end)
    dfActiveByDay <- dfActive[dfActive$overlap==TRUE, ]
    
    s3saveRDS(x = dfActiveByDay,
              object = paste0("CARS-5165/DailyActiveAds/Active_", substr(x, 1, 10), ".RDS"),
              bucket = s3BucketName)
}

for (i in daily){
  x <- as.POSIXlt(i, origin = '1970-01-01 00:00:00')
  fDay(x)
}
