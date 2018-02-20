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

dfAdPageLoads <- 
  s3readRDS(object = "CARS-5165/AdPageByDayAdId.RDS",
            bucket = s3BucketName)

dfAdImpressions <- 
  s3readRDS(object = "CARS-5165/AdImpressionsByDayAdId.RDS",
            bucket = s3BucketName)

dfShowPhone <- 
  s3readRDS(object = "CARS-5165/ShowingPhoneByDayAdId.RDS",
            bucket = s3BucketName)

dfMessages <- 
  s3readRDS(object = "CARS-5165/MessageSentByDayAdId.RDS",
            bucket = s3BucketName)

dfAdPageLoads$cd_adidv2 <- as.character(dfAdPageLoads$cd_adidv2)
dfAdImpressions$cd_adidv2 <- as.character(dfAdImpressions$cd_adidv2)
dfShowPhone$cd_adidv2 <- as.character(dfShowPhone$cd_adidv2)
dfMessages$cd_adidv2 <- as.character(dfMessages$cd_adidv2)

dfAll <-
  dfAdImpressions %>%
  left_join(dfAdPageLoads)%>%
  left_join(dfShowPhone)%>%
  left_join(dfMessages)
  
dfRepliesByAdID <- 
  as_tibble(
  dfAll %>%
  mutate(ad_id = as.numeric(cd_adidv2))%>%
  group_by(ad_id) %>%
  summarise(qtyAdImpressions = sum(qtyAdImpressions, na.rm = TRUE),
            qtyAdPageLoads = sum(qtyAdPageLoad, na.rm = TRUE),
            qtyShowPhone = sum(qtyShowPhone, na.rm = TRUE),
            qtyAdMessages = sum(qtyMessages, na.rm = TRUE)
            )
)

# save active ads df ----------------------------------------------------------
s3saveRDS(x = dfRepliesByAdID,
          object = "CARS-5165/dfRepliesByAdID.RDS",
          bucket = s3BucketName)
  