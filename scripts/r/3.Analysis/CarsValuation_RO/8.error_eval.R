#' CPP Romania
#' Combine all different models prediction into one unique dataframe
#' to be used for more in depth error evaluation (by make, model, etc.)
#' 
#' Error evaluation will be probably done via Tableau
#' 


# Working directory
setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")

library("dplyr")
library("aws.s3")


# Load rf test datasets
test_set_eval_rf <- read_csv("rf_data/test_set_eval_2018-05-21.csv")


#Load xgb test dataset
test_set_eval_xgb <- read_csv("xgb_data/test_set_eval_2018-05-21.csv") %>%
  select (pred_price_xgb, error_abs_xgb, error_per_xgb)


# Bind datasets 
##(no need to join as test dataset for both model has been split with same sample index)
dfTestErrorEval_RO <- cbind(test_set_eval_rf, test_set_eval_xgb)

rm(test_set_eval_rf, test_set_eval_xgb)


# Save binded test set for more in depth error evaluation (tbd via Tableau) ----------
write.csv(dfTestErrorEval_RO, file= paste0("datasets/dfTestErrorEval_RO", Sys.Date(), ".csv"))


# Also save it in S3 -----------------------------------------------------------------
## load personal credentials
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

bucket_path <- "s3://pyrates-data-ocean/"

## write to an in-memory raw connection
con <- rawConnection(raw(0), "r+")
write.csv(dfTestErrorEval_RO, con)

## upload the object to S3
aws.s3::put_object(file = rawConnectionValue(con), 
                   bucket = bucket_path, 
                   object = "datalake/autovitRO/CPP/dfTestErrorEval_RO.csv")

## close the connection
close(con)

  
