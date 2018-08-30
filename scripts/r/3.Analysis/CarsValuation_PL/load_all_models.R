# load libraries
library("data.table")
library("aws.s3")

# load credentials ------------------------------------------------------------
load("~/credentials.Rdata")

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#config
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"
vertical <- "autovitRO"

tmp_dir <- '~/tmp/AQS_20180609_144918/'

# lists ads files from datalake -----------------------------------------------
files <-
  list.files(path = tmp_dir, pattern = '.*model_results_.*_.*_.*_.*_.*_.*.RDS$',
             full.names = TRUE)

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(files, function (x){
    print(x) 
    data.table(readRDS(x))
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

# save file -------------------------------------------------------------------
id_dat <- as.character(as.hexmode(as.integer(Sys.time())))

saveRDS(object = dat, file = paste0(tmp_dir, id_dat, "_all_models_stats.RDS"))

# remove unnecessary files and free memory ------------------------------------
rm(list = c("files", "dat_list"))
gc()

s3saveRDS(x = dat,
          object = paste0(origin_bucket_prefix, tmp_dir, "all_models_stats.RDS"),
          bucket = origin_bucket_path
          )

# dat%>%
#   group_by(dataset, size, ntrees) %>%
#   summarise(meanMSE=mean(MSE)) %>% 
#   ggplot()+geom_point(aes(x=size, y=meanMSE, color=dataset))+
#   ylim(0, NA)
