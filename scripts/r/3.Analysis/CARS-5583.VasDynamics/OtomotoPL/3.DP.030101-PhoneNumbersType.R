library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("stringr")


# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "otomotoPL"
file <- "ads"

startDate <- "2017-01-01"
endDate <- as.character(Sys.Date()-1)

AllDates <- seq.Date(as.Date(startDate), as.Date(endDate), 1)

# getting file list -----------------------------------------------------------
s3ExistingsObjects <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = paste(sep = "/", "datalake", vertical, file, "RDS")
    )
  )

# check existing files --------------------------------------------------------

listExistingDates <-
  as.Date(str_extract(s3ExistingsObjects$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------

listDatesToGet <-
  as.character(AllDates[!(AllDates %in% listExistingDates)])









dfAds <-
  as_tibble(readRDS("~/CT/dfAds_OtomotoPL.RDS"))

dfUsers <-
  as_tibble(readRDS("~/CT/dfUsers_OtomotoPL.RDS"))

dfAdsPhones <-
  dfAds %>%
  inner_join(dfUsers, by = c("user_id" = "id")) %>%
  select(id, created_at_first, user_id, phone1, phone2, phone3)

# remove spaces ---------------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub(" ", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub(" ", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub(" ", "", dfAdsPhones$phone3)

# remove dash ---------------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("-", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("-", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("-", "", dfAdsPhones$phone3)

# remove country code ---------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("\\+48", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("\\+48", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("\\+48", "", dfAdsPhones$phone3)

# remove pljus signal  ---------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("\\+", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("\\+", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("\\+", "", dfAdsPhones$phone3)

# remove parenthesis ----------
dfAdsPhones$phone1 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone1)
dfAdsPhones$phone2 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone2)
dfAdsPhones$phone3 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone3)

# remove leading zero ----------
dfAdsPhones$phone1 <-
  gsub("^0", "", dfAdsPhones$phone1)
dfAdsPhones$phone2 <-
  gsub("^0", "", dfAdsPhones$phone2)
dfAdsPhones$phone3 <-
  gsub("^0", "", dfAdsPhones$phone3)

dfAdsPhones$PhoneNumber1 <- ""
dfAdsPhones$PhoneNumber2 <- ""
dfAdsPhones$PhoneNumber3 <- ""

dfAdsPhones$PhoneNumber1 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone1)
dfAdsPhones$PhoneNumber2 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone2)
dfAdsPhones$PhoneNumber3 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone3)

dfAdsPhones$PhoneNumber1[dfAdsPhones$phone1 == "" | is.na(dfAdsPhones$phone1)] <- "none"
dfAdsPhones$PhoneNumber2[dfAdsPhones$phone2 == "" | is.na(dfAdsPhones$phone2)] <- "none"
dfAdsPhones$PhoneNumber3[dfAdsPhones$phone3 == "" | is.na(dfAdsPhones$phone3)] <- "none"

dfAdsPhones$PhoneNumber1[dfAdsPhones$PhoneNumber1 == TRUE] <- "mobile"
dfAdsPhones$PhoneNumber2[dfAdsPhones$PhoneNumber2 == TRUE] <- "mobile"
dfAdsPhones$PhoneNumber3[dfAdsPhones$PhoneNumber3 == TRUE] <- "mobile"

dfAdsPhones$PhoneNumber1[dfAdsPhones$PhoneNumber1 == TRUE] <- "landline"
dfAdsPhones$PhoneNumber2[dfAdsPhones$PhoneNumber2 == TRUE] <- "landline"
dfAdsPhones$PhoneNumber3[dfAdsPhones$PhoneNumber3 == TRUE] <- "landline"
