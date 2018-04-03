# load libraries
library("data.table")
library("feather")

# lists ads files from datalake -----------------------------------------------
files <-
  list.files(path = '/data/lake', pattern = '^.*_ads_2.*feather$',
             full.names = TRUE)

# read all files to a list ---------------------------------------------------- 
dat_list <-
  lapply(files, function (x){
    print(x) 
    data.table(read_feather(x))
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  rbindlist(dat_list, use.names = TRUE, fill = TRUE)

# select only cars category ---------------------------------------------------
dfCarsAds <- dat[dat$category_id == 29, ]

# remove unnecessary files and free memory ------------------------------------
rm(list = c("files", "dat", "dat_list"))
gc()

dfCarsAds <- dfCarsAds[dfCarsAds$net_ad_counted == "1", ]


# save file -------------------------------------------------------------------
saveRDS(object = dfCarsAds, file = "~/tmp/RawHistoricalAds_OTO.RDS")

# remove objects from memory --------------------------------------------------
rm(list = ls())
gc()