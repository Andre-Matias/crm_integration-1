# clean workspace
rm(list=ls())

Rprof(tf <- "~/tmp/rprof.log", memory.profiling=TRUE)
Rprofmem(filename = "~/tmp/Rprofmem.out", append = FALSE, threshold = 0)

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
    read_tsv(file = x, progress = FALSE,
             col_names = c(
               "server_path",
               "year",
               "day",
               "month",
               "platform",
               "event",
               "touch_point_page",
               "ad_impressions_array"
             ),
             col_types = list(
               col_character(),
               col_character(),
               col_character(),
               col_character(),
               col_character(),
               col_character(),
               col_character(),
               col_character()
               )
             )
  }

# config ----------------------------------------------------------------------
origin_bucket_path <- "s3://pyrates-eu-data-ocean/"
destination_bucket_path <- "s3://pyrates-data-ocean/"

vertical <- "autovitRO"
filename <- "adsimpressions"

destination_bucket_prefix <- paste0("datalake/", vertical, "/", filename, "/")


# getting file list -----------------------------------------------------------
s3_files <- 
  as.data.frame(
    get_bucket(
      bucket = origin_bucket_path,
      max = Inf, prefix = "datalake/autovitRO/adsimpressions"
    )
  )

s3_files_destination <- 
  as.data.frame(
    get_bucket(
      bucket = destination_bucket_path,
      max = Inf, prefix = "datalake/autovitRO/adsimpressions"
    )
  )

dates_origin <- unique(str_extract(s3_files$Key[s3_files$Size > 0], "[0-9]{8}"))

dates_destination <- unique(str_extract(s3_files_destination$Key, "[0-9]{8}"))

dates <- dates_origin[!(dates_origin %in% dates_destination)]

for(date in dates){
  
fileToRead <-
  s3_files$Key[s3_files$Size > 0
               & grepl(date, s3_files$Key)]

print(paste(Sys.time(), " | ", fileToRead))

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(fileToRead, function (x){
    print(x)
    s3read_using(FUN = s3ReadFileTab, object = x, bucket = origin_bucket_path )
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

rm(dat_list)
gc()

print(paste(Sys.time(), " | ", "Cleaning ads impressions array..."))

dat$ad_impressions_array <- gsub("\\[", "", dat$ad_impressions_array)
dat$ad_impressions_array <- gsub("\\]", "", dat$ad_impressions_array)
dat$ad_impressions_array <- gsub("\\{\\}", "", dat$ad_impressions_array)
dat$ad_impressions_array <- gsub("\"", "", dat$ad_impressions_array)

dat <- dat[dat$ad_impressions_array != "", ]

dat <- as_tibble(dat)

print(paste(Sys.time(), " | ", "Summarizing data..."))

dat <-
  dat %>%
  unnest(ad_id = strsplit(ad_impressions_array, split = ",")) %>%
  select(-ad_impressions_array) %>%
  group_by(year, month, day, ad_id) %>%
  summarise(qtyAdImpressions = sum(n())) %>%
  group_by() %>%
  mutate(day = as.Date(paste(year, month, day, sep="-"))) %>% 
  select(-year, -month)

print(paste(Sys.time(), " | ", "Start saving to AWS..."))

s3saveRDS(x = dat, 
          object = paste0(destination_bucket_prefix, vertical, "_",filename, "_",date,".RDS"), 
          bucket = destination_bucket_path
          )

print(paste(Sys.time(), " | ", "Saved to AWS! NEXT!!!!"))

dat <- NULL
gc()

Rprof(NULL)
summaryRprof(tf)
}


