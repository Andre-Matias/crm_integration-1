
# load libraries --------------------------------------------------------------
library("aws.s3")
library("dplyr")
library("magrittr")
library("dtplyr")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")
library("stringr")
library("DescTools")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# list all parameters files----------------------------------------------------
s3Files_Ads <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/autovitRO/ads/RDS/")
  )

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(s3Files_Ads$Key, function (x){
    print(Sys.time())
    print(x)
    data.table(
      s3readRDS(x, bucket = bucket_path)
      )
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  as_tibble(rbindlist(dat_list, use.names = TRUE, fill = TRUE))

# select 

dat <-
  dat %>%
  filter(category_id %in% c(29, 81), net_ad_counted == 1) %>%
  mutate(created_at_first_day = as.Date(created_at_first),
         ad_id = as.character(id)
  ) %>%
  select(-created_at_first, -id) %>%
  filter(created_at_first_day >= as.Date('2017-10-01')) %>%
  select(ad_id, created_at_first_day)

# save to aws

 s3saveRDS(
   x = dat, 
   object = "datalake/autovitRO/AIO/Ads_AIO.RDS",
   bucket = bucket_path
 )