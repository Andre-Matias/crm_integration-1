#load libraries
library("aws.s3")
library("feather")
library("dplyr")
library("data.table")
library("dtplyr")
library("fasttime")
library("magrittr")
library("ggplot2")

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

s3ReadAndPrint <- 
  function(x){
    print(x)
    s3read_using(
      FUN = read_feather, object = x, bucket = s3BucketName
    )
}

s3ReadAndSummarise <-
  function(x,y,z){
    print(x)
    tmp <- 
      s3readRDS(object = x, bucket = s3BucketName)
    
    name <- 
      gsub(y,z, x)
    print(name)
    
    dfTmp <-  
      tmp %>%
      group_by(d_time_date, cd_adidv2) %>%
      summarise(qtyMessages = sum(cm_32399)) %>%
      group_by()%>%
      mutate(d_time_date = fastPOSIXct(d_time_date))
    
    s3saveRDS(x = dfTmp,
              object = name,
              bucket = s3BucketName)
  }

# read feather files ----------------------------------------------------------

file.list <- as.data.frame(
  get_bucket(bucket = s3BucketName,
             prefix = "daniel.rocha/RIX/RAW/message_sent/ati_574113_message_s")
)

# save feather as RDS ---------------------------------------------------------
lapply(file.list$Key,
       function(x) s3FeatherToRDS(x, "/message_sent/", "/message_sent/RDS/"))

# read RDS files --------------------------------------------------------------
file.list.RDS <- as.data.frame(
  get_bucket(
    bucket = s3BucketName,
    prefix = "daniel.rocha/RIX/RAW/message_sent/RDS/ati_")
)

# summarise RDS By Day and AdID -----------------------------------------------
lapply(file.list.RDS$Key, 
       function(x) s3ReadAndSummarise(x, 
                                      y = "/message_sent/RDS/ati", 
                                      z = "/message_sent/RDS/ByDayAdID/ati"))

# read summarised files  ------------------------------------------------------

file.list.RDS.sum <- as.data.frame(
  get_bucket(bucket = s3BucketName,
             prefix = "daniel.rocha/RIX/RAW/message_sent/RDS/ByDayAdID/")
)

# join all files in one list --------------------------------------------------
dat_list <-
  lapply(file.list.RDS.sum$Key, function(x) s3readRDS(x, bucket = s3BucketName)
         )

# list to data.frame ----------------------------------------------------------
dat <- 
  as_tibble(rbindlist(dat_list, fill = TRUE))

# save final data to s3 -------------------------------------------------------
df <-
  dat %>%
  filter(d_time_date >= '2017-01-01 00:00:00',
         d_time_date < '2017-07-01 00:00:00')

s3saveRDS(x = df,
          object = "CARS-5165/MessageSentByDayAdId.RDS",
          bucket = s3BucketName)

