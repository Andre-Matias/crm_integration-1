#'
#' Cars Price Prediction
#' Explore dataset previous to modeling
#' Dataset has been previpusly cleaned and prepared to be used for modeling (output from step 4)
#' 


# Load libraries
library(tidyverse)
library(corrplot)

# setwd("~/Documents/price_prediction")

# Load dataset
dfDataForModel <- read_csv("dfDataForModel.csv")

# Look at data
glimpse(dfDataForModel)
sum(is.na(dfDataForModel))

# Check for skewed distributions >> might need log transformations
## ignore ad_id,damaged 
hists <- dfDataForModel %>%
  select(-ad_id) %>%
  keep(is.numeric) %>% 
  gather() %>% 
  ggplot(aes(value)) +
  facet_wrap(~ key, scales = "free") +
  geom_histogram() 
plot(hists)

table(dfDataForModel$engine_capacity)

dfDataForModel %>%
  ggplot(aes(engine_capacity)) + geom_histogram()

## age doesn't seem skewed 
dfDataForModel %>%
  ggplot(aes(age)) + geom_histogram()

dfDataForModel %>%
  ggplot(aes(mileage)) + geom_histogram()

dfDataForModel %>%
  ggplot(aes(engine_power)) + geom_histogram()


# Check for correlations >> if collinearity might remove variables
## doesn't seem to have collinearity between other variables, except for year and age...
cor_table <- dfDataForModel %>%
  select(-ad_id) %>%
  keep(is.numeric) %>%
  cor()
round(cor_table, 2)

# cor_mat <- corrplot(as.matrix(test), method="circle", is.corr=F)

## relationship with price for categorical variables
dfDataForModel %>%
  ggplot(aes(x=make, y=price_RON)) + geom_boxplot() + coord_flip() 

dfDataForModel %>%
  ggplot(aes(x=body_type, y=price_RON)) + geom_boxplot() + coord_flip() 

dfDataForModel %>%
  ggplot(aes(x=gearbox, y=price_RON)) + geom_boxplot() + coord_flip() 

dfDataForModel %>%
  ggplot(aes(x=fuel_type, y=price_RON)) + geom_boxplot() + coord_flip()

dfDataForModel %>%
  ggplot(aes(x=as.factor(nr_seats), y=price_RON)) + geom_boxplot() + coord_flip()

## age vs price looks logarithmic
dfDataForModel %>%
     ggplot(aes(x=as.numeric(age), y=price_RON)) + geom_point() + geom_smooth()

dfDataForModel %>%
  ggplot(aes(x=as.numeric(age), y=price_RON)) + geom_point()  +
  facet_wrap(~make)

## also mileage vs price looks logarithmic
sample_n(dfDataForModel, 50000) %>%
  ggplot(aes(x=as.numeric(mileage), y=price_RON)) + geom_point() + geom_smooth()

# After cleaning engine capacity!
sample_n(dfDataForModel,50000) %>%
  ggplot(aes(x=as.numeric(engine_capacity), y=price_RON)) + geom_point() + geom_smooth()
