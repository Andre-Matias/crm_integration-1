#' ############################################################################
#' Build price distributions for relevant combinations of cars
#' 
#' important parameters according to var. imp in XGBoost:
#' make, model, year, mileage, engine_power
#' 
#' market: portugal
#' ############################################################################

setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_PT")

library(tidyverse)



# Load dataset ready for modelling (a compressed file) ------------------------
df <- read.table(gzfile("datasets/preprocessing_data_wo_encoding.gz"), 
                 sep=",", header=TRUE) 


# Recalculate year as it's missing
df$year <- 2018 - df$age

# See range for numeric variables
summary(df)

# Normalize mileage using breaks of 50k kilometers ----------------------------
breaks <- seq(min(df$mileage, na.rm=T), max(df$mileage, na.rm=T), 50000)
df$mileage_cut <- cut(df$mileage, breaks, dig.lab=10)
df$mileage_cut <- str_replace(df$mileage_cut, "]", ")")   # replacing ] with )

## check how many ads fall into outliers buckets or NAs
df %>%
  group_by(mileage_cut) %>%
  summarize( count= n()) %>%
  #mutate(freq = count/sum(count) *100) %>%
  arrange(desc(count)) %>%
  View()

## Check distribution just one make-model
opel_distr <- df %>%
  filter (make=="opel" & model =="zafira")
dim(opel_distr)
quantile(opel_distr$price)
summary(opel_distr$price)


# Calculate distribution statistics -------------------------------------------
price_distr <- df %>%
  group_by(make, model, year, mileage_cut) %>%
  summarize(min_price = min(price),
            twentyfive_quantile = quantile(price, probs=0.25),
            median = median(price),
            seventyfive_quantile = quantile(price, probs=0.75),
            max_price = max(price),
            avg = mean(price),
            #good_price_range = round((first_quantile/avg) -1, 2),
            #forty_quantile = quantile(price, probs=0.40),
            #sixty_quantile = quantile(price, probs=0.60),
            #good_price_range_2 = round((forty_quantile/avg) -1, 2),
            thirtyfive_quantile = quantile(price, probs=0.35),
            sixtyfive_quantile = quantile(price, probs=0.65),
            good_price_range_min = round((thirtyfive_quantile/avg) -1, 2),
            good_price_range_max = round((sixtyfive_quantile/avg) -1, 2),
            #good_price_range_median = round((first_quantile/median) -1, 2),
            count = n()
  ) %>%
  arrange(desc(count))

dim(price_distr)
summary(price_distr$good_price_range_min)  # on avg. 3.3% below average price
summary(price_distr$good_price_range_max)  # on avg. 2.7% above average price


# How many ads we will cover if we took only a "significant" (min number of observations) distribution?
find_ads_coverage <- function(df, min_count=50){
  min_count = min_count
  tot_ads = sum(df$count)
  ads_to_consider <- sum(df[df$count>=min_count, ]$count)
  ads_coverage =ads_to_consider / tot_ads 
  return(ads_coverage)
  
}

find_ads_coverage(price_distr) # use default value of 50 obs.
# make + model + year + mileage: 52%

find_ads_coverage(price_distr, 30) # reducing to 30, the coverage would raise to 65%




# Calculate distribution statistics for fall-back 1 dataset -------------------
price_distr_back1 <- df %>%
  group_by(make, model, year) %>% # removing grouping by mileage
  summarize(min_price = min(price),
            twentyfive_quantile = quantile(price, probs=0.25),
            median = median(price),
            seventyfive_quantile = quantile(price, probs=0.75),
            max_price = max(price),
            avg = mean(price),
            #good_price_range = round((first_quantile/avg) -1, 2),
            #forty_quantile = quantile(price, probs=0.40),
            #sixty_quantile = quantile(price, probs=0.60),
            #good_price_range_2 = round((forty_quantile/avg) -1, 2),
            thirtyfive_quantile = quantile(price, probs=0.35),
            sixtyfive_quantile = quantile(price, probs=0.65),
            good_price_range_min = round((thirtyfive_quantile/avg) -1, 2),
            good_price_range_max = round((sixtyfive_quantile/avg) -1, 2),
            #good_price_range_median = round((first_quantile/median) -1, 2),
            count = n()
  ) %>%
  arrange(desc(count))

dim(price_distr_back1)
summary(price_distr_back1$good_price_range_min)  # on avg. 4.4% below average price
summary(price_distr_back1$good_price_range_max)  # on avg. 3.6% above average price


# How many ads we will cover if we took only a "significant" (min number of observations) distribution?
find_ads_coverage(price_distr_back1)
# make + model + year : 79%




# Calculate distribution statistics for fall-back 2 dataset -------------------
price_distr_back2 <- df %>%
  group_by(make, model) %>% # removing grouping by mileage, year
  summarize(min_price = min(price),
            twentyfive_quantile = quantile(price, probs=0.25),
            median = median(price),
            seventyfive_quantile = quantile(price, probs=0.75),
            max_price = max(price),
            avg = mean(price),
            #good_price_range = round((first_quantile/avg) -1, 2),
            #forty_quantile = quantile(price, probs=0.40),
            #sixty_quantile = quantile(price, probs=0.60),
            #good_price_range_2 = round((forty_quantile/avg) -1, 2),
            thirtyfive_quantile = quantile(price, probs=0.35),
            sixtyfive_quantile = quantile(price, probs=0.65),
            good_price_range_min = round((thirtyfive_quantile/avg) -1, 2),
            good_price_range_max = round((sixtyfive_quantile/avg) -1, 2),
            #good_price_range_median = round((first_quantile/median) -1, 2),
            count = n()
  ) %>%
  arrange(desc(count))

dim(price_distr_back2)
summary(price_distr_back2$good_price_range_min)  # on avg. 10.3% below average price
summary(price_distr_back2$good_price_range_max)  # on avg. 7% above average price

# How many ads we will cover if we took only a "significant" (min number of observations) distribution?
find_ads_coverage(price_distr_back2)
# make + model: 97%


# Split mileage into min and max in first dataset -----------------------------
price_distr_m <- separate(data = price_distr, col = mileage_cut, into = c("mileage_min", "mileage_max"), sep=",")   # store it in a new df
price_distr_m$mileage_min <- gsub("[()]", "", price_distr_m$mileage_min)
price_distr_m$mileage_max <- gsub("[()]", "", price_distr_m$mileage_max)


# Bind datasets into one long table -------------------------------------------
price_distr_all_pt <- bind_rows(price_distr_m, price_distr_back1, price_distr_back2)
dim(price_distr_all_pt)
write.csv(price_distr_all_pt, file= "datasets/price_distr_all_pt.csv")


#price_distr_all_pt <- read_csv("datasets/price_distr_all_pt.csv")


