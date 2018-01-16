arrange_.party_df <- function (.data, ..., .dots = list()) 
{
  multidplyr:::shard_call(.data, quote(dplyr::arrange), ..., .dots = .dots, 
                          groups = .data$groups[-length(.data$groups)])
}

# load libraries --------------------------------------------------------------
library("aws.s3")
library("multidplyr")
library("magrittr")
library("dplyr")
library("dtplyr")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

# read file -------------------------------------------------------------------

df <- 
  s3readRDS(
    bucket = bucket_path, 
    object = "daniel.rocha/RIX/RAW/ads_history/RDS/ads_activity/all.RDS"
    )

cluster <- create_cluster(7)

cluster %>% 
  cluster_library("magrittr") %>%
  cluster_library("dplyr") %>%
  cluster_library("data.table") %>%
  cluster_library("dtplyr") %>%
  cluster_eval(search())

set_default_cluster(cluster)

dfActive <-
  df %>%
  partition(id, cluster = cluster) %>%
  arrange(id, primary) %>%
  mutate(
    next.id = lead(id, order_by=primary)
    ,next.primary = lead(primary, order_by=primary)
    ,next.changed_at = lead(changed_at, order_by=primary)
    ,next.status = lead(status, order_by=primary)
    ) %>%
  collect() %>%
  arrange(id, primary) %>%
  filter(status=="active")

range_start <- as.POSIXlt("2016-01-01 00:00:00")
range_end <- as.POSIXlt(format(Sys.time(), "%Y-%m-%d 00:00:00"))

daily <- format(seq(from = range_start,to = range_end, by='day'), "%Y-%m-%d 00:00:00")