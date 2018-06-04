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

# user-defined functions ------------------------------------------------------
s3ReadFileTab <-
  function(x){
    read_tsv(file = x, col_names = FALSE)
  }

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-eu-data-ocean/"

# getting file list -----------------------------------------------------------
s3_files <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/autovitRO/adsimpressions"
    )
  )

listFilesToRead <-
  s3_files$Key[s3_files$Size > 0
               & grepl("20171001", s3_files$Key)]

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(listFilesToRead, function (x){
    print(x)
    s3read_using(FUN = s3ReadFileTab, object = x, bucket = bucket_path)
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

dat$X8 <- gsub("\\[", "", dat$X8)
dat$X8 <- gsub("\\]", "", dat$X8)
dat$X8 <- gsub("\\{\\}", "", dat$X8)
dat$X8 <- gsub("\"", "", dat$X8)

dat <- dat[dat$X8 != "", ]

dat <- as_tibble(dat)

dat <-
  head(dat, 2) %>%
  unnest(ad_id = strsplit(X8, split = ",")) %>%
  select(-X8)