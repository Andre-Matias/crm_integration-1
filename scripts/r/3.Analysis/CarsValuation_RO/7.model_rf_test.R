#' CPP Romania
#' Predict values on test dataset and evaluate error
#' 
#' error: error % and MAE
#' error % seem more appropriate for price prediction
#'

# setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")

library("ranger")
library("scales")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")


# Build final model using the entire training dataset -------------------------

## Selected predictors
selected_predictors <-
  c("make", "mileage", "age", "model", "engine_power","body_type", 
    "gearbox", "engine_capacity", "fuel_type")

formula <- paste0("price_RON~", paste(selected_predictors, collapse="+"))
model_formula <- as.formula(formula)


RF_model_best <- 
  ranger(formula = model_formula,
         data =  dfDataForModel_train,
         num.trees = 500, num.threads = 8, verbose = TRUE, 
         importance = "impurity", 
         min.node.size = 5
  )

# Predict price using test set ------------------------------------------------
pred <- predict(RF_model_best, data = dfDataForModel_test)
dfDataForModel_test$pred_price <- pred$predictions


# Calculate error -------------------------------------------------------------
## MAE: mean absolute error
dfDataForModel_test$error_abs <- 
  abs(dfDataForModel_test$price_RON - dfDataForModel_test$pred_price)

mean(dfDataForModel_test$error_abs)   # 8300 RON ~= 1790 €


## error_per: mean of error % over the price posted in the ad
dfDataForModel_test$error_per <-
  dfDataForModel_test$error_abs / dfDataForModel_test$price_RON
mean(dfDataForModel_test$error_per)   # 17.2%


#' Conclusions:
#' The avg. error % was 17.2% when testing the best RF model on a new test set
#' 
#' country error comparison:
#' PL: 19.6% 


