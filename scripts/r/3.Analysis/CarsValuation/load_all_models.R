# load libraries
library("data.table")

dir <- '~/tmp/AQS_20180609_082653/'

# lists ads files from datalake -----------------------------------------------
files <-
  list.files(path = dir, pattern = '.*model_results_.*_.*_.*_.*_.*_.*.RDS$',
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

saveRDS(object = dat, file = paste0(dir, id_dat, "_all_models_stats.RDS"))

# remove unnecessary files and free memory ------------------------------------
rm(list = c("files", "dat_list"))
gc()



# dat%>%
#   group_by(dataset, size, ntrees) %>%
#   summarise(meanMSE=mean(MSE)) %>% 
#   ggplot()+geom_point(aes(x=size, y=meanMSE, color=dataset))+
#   ylim(0, NA)
