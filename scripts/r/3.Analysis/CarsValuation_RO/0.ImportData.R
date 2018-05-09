#' ################################################################################################
#' note: eventually dataset will be available with created_at_first field and filtered with only live ads
#' 
#' 1) Import data with all ads
#' 2) tmp step: left_join with other dataset to get created_at_first field
#' 3) Import data with ads and net counted field to be able to filter only live ads
#' 4) Inner join 1 and 3 to keep only live ads
#' 
#' 
#' ################################################################################################

#load librabries --------------------------------------------------------------
library("aws.s3")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")

# Load credentials -------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")


Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)


# 1) ===

# Load ads dataset with predictors and response -------------------------------
# it has both live and draft ads
ads_all_param <-
  as.data.table(
    s3readRDS(object = "datalake/autovitRO/AIO/AdsParametersWIDE_AIO.RDS",
              bucket = "s3://pyrates-data-ocean/"
    )
  )


# 2) === tmp step

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
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)
#saveRDS(dat, "dat.rds")

# Select only required fields
dat2<- dat %>%
  select (id, created_at_first)

# Left join to get the created_at_first field
ads_all_param <- left_join(ads_all_param, dat2, by= c("ad_id"="id"))


# 3) ===

ads_net_counted<- readRDS("dat.rds") %>%
  select(id, net_ad_counted)

table(ads_net_counted$net_ad_counted) # 74% of ads went live
ads_live <- filter(ads_net_counted, net_ad_counted=="1") %>%
              rename(ad_id=)



# 4) === Finally keep only live ads with parameters

dfCarsAdsWideWithPrice <- inner_join(ads_all_param, ads_live, by= c("ad_id"="id"))


rm(ads_net_counted, ads_live, dat, ads_all_param)



# Resume of cleaning:
# data since Jan 1 2017, 16 months of data
# starting dataset: ads_all_param -> 800k
# filtering live: dfCarsAdsWideWithPrice -> 620k
