# load libraries --------------------------------------------------------------
library("aws.s3")

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
      max = Inf, prefix = "daniel.rocha/RIX/RAW/ads_history/RDS/"
      )
  )

s3_ads_history_files_ads_activity <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "daniel.rocha/RIX/RAW/ads_history/RDS/ads_activity/"
    )
  )

objects_missings <-
  s3_ads_history_files$Key[
    !(s3_ads_history_files$Key %in% s3_ads_history_files_ads_activity$Key)
    ]
  
for(i in objects_missings){
dfTmp <-
  s3readRDS(bucket = bucket_path, i)

dfTmp <-
  dfTmp[!is.na(dfTmp$status), c("primary", "changed_at", "id", "status")]


s3saveRDS(x = dfTmp, 
          bucket = bucket_path, 
          object = gsub(
            "daniel.rocha/RIX/RAW/ads_history/RDS/RDL_OtomotoPL_ads_history_",
            "daniel.rocha/RIX/RAW/ads_history/RDS/ads_activity/RDL_OtomotoPL_ads_history_",
            i
            )
)
}