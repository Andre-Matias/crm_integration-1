#load libraries
library("aws.s3")
library("feather")

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
  rbindlist(dat_list, fill = TRUE)