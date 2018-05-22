#' CPP Romania
#' Predict values on test dataset and evaluate error using RF best model
#' 
#' error: error % and MAE
#' error % seem more appropriate for price prediction
#'

# Working directory
setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")

library("ranger")
library("scales")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("aws.s3")


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
pred_rf <- predict(RF_model_best, data = dfDataForModel_test)
dfDataForModel_test$pred_price_rf <- pred_rf$predictions


# Calculate error -------------------------------------------------------------
## MAE: mean absolute error
dfDataForModel_test$error_abs_rf <- 
  abs(dfDataForModel_test$price_RON - dfDataForModel_test$pred_price_rf)

mean(dfDataForModel_test$error_abs_rf)   # 7738 RON ~= 1673 €


## error_per: mean of error % over the price posted in the ad
dfDataForModel_test$error_per_rf <-
  dfDataForModel_test$error_abs_rf / dfDataForModel_test$price_RON
mean(dfDataForModel_test$error_per_rf)   # 16.05%


#' Conclusions:
#' The avg. error % was about 16% when testing the best RF model on a new test set
#' 
#' country error comparison:
#' PL: 19.6% 



# Save test set for more in depth error evaluation (tbd via Tableau) ----------
write.csv(dfDataForModel_test, file= paste0("rf_data/test_set_eval_", Sys.Date(), ".csv"))


# Save RF model to S3 ---------------------------------------------------------

## load personal credentials
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

bucket_path <- "s3://pyrates-data-ocean/"
           
s3saveRDS(
  x = RF_model_best, 
  object = "datalake/autovitRO/CPP/RF_model_best.RDS",
  bucket = bucket_path
)


# Check variables importance for predicting -----------------------------------
importance(RF_model_best)
sort(RF_model_best$variable.importance, decreasing=TRUE)
##' in order or importance:
##' 1) engine_power
##' 2) age
##' 3) engine_capacity
##' 4) mileage
##' 

imp<- sort(RF_model_best$variable.importance, decreasing=TRUE)
imp<- data.frame(variable=names(imp), importance=imp) 
ggplot(imp, aes(x=reorder(variable, -importance), y=importance)) +
  geom_point() 




# some use cases: -------------------------------------------------------------
# predict your own combination
test<- sample_n(dfDataForModel_test,1) %>%
  select (2:4, 6:11)
test
new_row<- data.frame(make="opel",
                model="zafira",
                mileage=180000,
                body_type="minivan",
                engine_capacity=1796,
                engine_power=300,
                gearbox=c("manual"),
                fuel_type="petrol",
                age=16)
test<- rbind(test, new_row)
test
predict_test <- predict(RF_model_best, data=test)
predict_test$predictions
