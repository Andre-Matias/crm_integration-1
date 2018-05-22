#' CPP Romania
#' Predict values on test dataset and evaluate error using xgboost best model
#' 
#' error: error % and MAE
#' error % seem more appropriate for price prediction
#'


# Working directory
setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")

# Required libraries
library("xgboost")
library("Matrix")
library("tidyverse")


# Build final model using entire training dataset -----------------------------
XGB_model_best <- xgboost(data = d_train,   # d_train was created in "model_xg_tune.R"
                          max_depth = 12, 
                          eta = 1, 
                          nthread = 2, 
                          nrounds = 20, 
                          objective = "reg:linear",
                          early_stopping_rounds = 3)


# Predict price using test set ------------------------------------------------
pred_price_xgb <- predict(XGB_model_best, d_test)   # d_test was created in "model_xg_tune.R"

results <- as.data.frame(cbind(test_data, test_label, pred_price_xgb)) # test_data test_label were created in "model_xg_tune.R"


# Calculate error -------------------------------------------------------------
## MAE: mean absolute error
results$error_abs_xgb <- 
  abs(results$price_RON - results$pred_price_xgb)

mean(results$error_abs_xgb)   # 6626 RON ~= 1433 €
## vs 1790€ with RF


## error_per: mean of error % over the price posted in the ad
results$error_per_xgb <-
  results$error_abs_xgb / results$price_RON

mean(results$error_per_xgb)   # 14.86%
## vs 16% with RF


#' Conclusions:
#' The avg. error % is 14.8% when testing the best XGB model on a new test set. 
#' RF had an error % of 16% on the same test dataset.
#' 
#' Countries error comparison:
#' Poland: 
#' - RF: 19.6%
#' - XGB:


# Save test set for more in depth error evaluation (tbd via Tableau) ----------
write.csv(results, file= paste0("xgb_data/test_set_eval_", Sys.Date(), ".csv"))


# Save RF model to S3 ---------------------------------------------------------

## load personal credentials
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)


bucket_path <- "s3://pyrates-data-ocean/"

s3saveRDS(
  x = XGB_model_best, 
  object = "datalake/autovitRO/CPP/XGB_model_best.RDS",
  bucket = bucket_path
)

