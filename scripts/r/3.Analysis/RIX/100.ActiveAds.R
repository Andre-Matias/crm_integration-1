# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

# getting file list -----------------------------------------------------------
s3_ads_history_files <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "daniel.rocha/RIX/RAW/ads_history/RDS/ads_activity/"
    )
  )

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(s3_ads_history_files$Key, function (x){
    x <- paste0(bucket_path, x)
    print(x)
    data.table(s3readRDS(x))
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

s3saveRDS(x = dat, 
          bucket = bucket_path, 
          object = "daniel.rocha/RIX/RAW/ads_history/RDS/ads_activity/all.RDS"
            )
