# load libraries --------------------------------------------------------------
library("aws.s3")
library("magrittr")
library("dplyr")
library("data.table")
library("dtplyr")
library("readr")
library("stringr")
library("tidyr")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# config ----------------------------------------------------------------------
vertical <- "autovitRO"
filename <- "adsimages"

origin_bucket_path <- "s3://pyrates-data-ocean/"
destination_bucket_path <- "s3://pyrates-data-ocean/"

origin_bucket_prefix <- paste0("datalake/", vertical, "/", filename, "/")
destination_bucket_prefix <- paste0("datalake/", vertical, "/", "AIO", "/")

# getting file list -----------------------------------------------------------
s3_files <- 
  as.data.frame(
    get_bucket(
      bucket = origin_bucket_path,
      max = Inf, prefix = origin_bucket_prefix
    )
  )

#read all files to list -------------------------------------------------------
dat_list <-
  lapply(s3_files$Key, function (x){
    print(x)
    s3readRDS(object = x, bucket = origin_bucket_path )
  }
  )
  
# merge all data frames from the list to a single data frame ------------------
dat <-
  as_tibble(rbindlist(dat_list, use.names = TRUE, fill = TRUE))

dat <-
  dat %>%
  mutate(nr_images = unlist(nr_images))

#free up memory ---------------------------------------------------------------
rm(dat_list)
gc()

# save to AWS -----------------------------------------------------------------
s3saveRDS(x = dat, 
          object = paste0(destination_bucket_prefix,
                          vertical, "_", filename, "_AIO", ".RDS"), 
          bucket = destination_bucket_path
)