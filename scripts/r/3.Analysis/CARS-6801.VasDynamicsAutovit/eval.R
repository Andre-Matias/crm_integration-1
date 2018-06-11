# load libraries --------------------------------------------------------------
library("aws.s3")
library("magrittr")
library("dplyr")
library("data.table")
library("dtplyr")
library("readr")
library("stringr")
library("tidyr")
library("ggplot2")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#config
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"
vertical <- "autovitRO"

df <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "~/tmp/AQS_20180609_144918/all_models_stats.RDS"), bucket = origin_bucket_path)
  )

dfVariableImportance <- df[df$dataset == "test" & !is.na(df$variable.importance.topads),
                           colnames(df)[grepl("variable.importance", colnames(df))]]

dfVariableImportance_Stats <-
  dfVariableImportance %>%
  gather()

ggplot(dfVariableImportance_Stats)+
  geom_boxplot(aes(key, value))+ coord_flip()
