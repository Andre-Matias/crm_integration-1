f1 <- read.csv2("~/Downloads/query_result.csv", sep = ",")
f2 <- read.csv2("~/Downloads/query_result10-20.csv", sep = ",")
f3 <- read.csv2("~/Downloads/query_result21-30.csv", sep = ",")

f <- rbind(f1, f2)
f <- rbind(f, f3)

saveRDS(f, "~/tmp/AtlasStatisticsEvents.RDS")

stats <-
  f %>%
  group_by(ad_id) %>%
  summarise(view_results = sum(view_results),
            view_ad_page = sum(view_ad_page),
            ctr = view_ad_page / view_results
            )

boxplot(stats$ctr[stats$ctr < 1 & stats$ctr > 0])

> summary(stats$ctr[stats$ctr < 1 & stats$ctr > 0])
Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
0.001   0.058   0.107   0.202   0.276   0.997    5243 