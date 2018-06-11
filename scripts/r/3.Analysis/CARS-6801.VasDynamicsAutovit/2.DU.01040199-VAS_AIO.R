
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
library("readr")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#remove trash 
rm(list = setdiff(ls(), c("myS3key", "MyS3SecretAccessKey")))

# config ----------------------------------------------------------------------
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"

dfAds <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "Ads_AIO.RDS"), bucket = origin_bucket_path)
  )

dfVas <- 
  read_tsv("/tmp/autovit_VAS.txt", col_names = c("ad_id", "date", "typeVAS", "qtyVAS"),
           col_types = list(
             col_character(),
             col_date(),
             col_character(),
             col_integer()
             )
           )

dfAutomaticBump <- 
  read_tsv("/tmp/autovit_automatic_bump_VAS.txt", col_names = c("ad_id", "date", "qtyVAS"),
           col_types = list(
             col_character(),
             col_date(),
             col_integer()
             )
           )

dfAutomaticBump$typeVAS <- "bump_up"

dfAutomaticBump <- dfAutomaticBump [, c("ad_id", "date", "typeVAS", "qtyVAS")]

dfAllVAS <- rbind(dfVas, dfAutomaticBump)

dfTmp <-
  dfAds %>% 
  left_join(dfAllVAS, by = c('ad_id')) %>%
  filter(!is.na(date)) %>%
  mutate(d = difftime(date, created_at_first_day, units = "day")) %>%
  filter(d >= 0, d < 7) %>%
  group_by(ad_id, typeVAS) %>%
  summarise(qtyVAS = sum(qtyVAS)) %>%
  group_by() %>%
  spread(key = typeVAS, value = qtyVAS, fill = 0)


s3saveRDS(x = dfTmp,
          object = paste0(origin_bucket_prefix, "VAS_AIO.RDS"), 
          bucket = origin_bucket_path
)