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
    prefix = "CARS/CARS-5597/v-s")
)

# join all files in one list --------------------------------------------------
dat_list <-
  lapply(file.list.RDS$Key, function(x) s3readRDS(x, bucket = s3BucketName)
  )

# list to one dataframe -------------------------------------------------------
dat <- 
  as_tibble(rbindlist(dat_list, fill = TRUE))

users <- read.table("~/autovit.tsv", header = TRUE)

dat <- dat[!(dat$user_id %in% users$user_id), ]

dat$resultset_page_number[is.na(dat$resultset_page_number)] <- 0 

dat <- dat[dat$resultset_page_number %in% c("0", "1"), ]

df <- dat

df$nrow <- seq(1, nrow(df), 1)

df$extra <- as.character(df$extra)

df$extra <- gsub("\\[\\]", "\"\"", df$extra)

df$extra <- lapply(df$extra, function(x) jsonlite::fromJSON(x))

df_extra <- df[, c("extra")]

df_extra$nrow <- seq(1, nrow(df_extra), 1)

a <- melt(df_extra$extra, na.rm = TRUE)

b <- unique(a$L2)

filters <- 
  c("brand", "model", "from_year", "item_condition", "equipment", "to_mileage",
  "from_mileage",  "to_year", "fuel_type", "from_cm3", "to_cm3",
  "damage", "from_price", "to_price", "city_name", "distance_filt", "subregion_id",
  "region_name", "subregion_name", "color", "section", "only_private", "cat_l2_name",
  "only_pros", "metallic", "district_name", "particle_filter", "brand_program", 
  "authorized_dealer", "invs", "invc", "body_type"
  )

d <- a[a$L2 %in% filters, c("L1", "L2")]

saveRDS(d, "~/d_stv_buyers.RDS")