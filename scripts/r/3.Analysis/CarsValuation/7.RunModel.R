library("ranger")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("hydroGOF")
library("tidyr")
library("caret")

id <- as.character(as.hexmode(as.integer(Sys.time())))

#for(s in seq(0.1,1,0.1)){
dfDataForModel <- readRDS("~/tmp/RawHistoricalAds_OTO_main_AIO_wPriceFMO.RDS")
#  learn_idx <- sample(x = nrow(dfDataForModel), size =   s * nrow(dfDataForModel))
#dfDataForModel <- dfDataForModel[learn_idx, ]

  dfDataForModel$log_age <- log(dfDataForModel$age)
  dfDataForModel$log_mileage <- log(dfDataForModel$mileage)
  
# reserve 20% for test
test_idx <- sample(x = nrow(dfDataForModel), size = (1 - 1/6) * nrow(dfDataForModel))

dfDataForModel_train <- dfDataForModel[test_idx, ]
dfDataForModel_test <- dfDataForModel[-test_idx, ]

#save(object = dfDataForModel_train, file = "~/tmp/dfDataForModel_train.RDS")
#save(object = dfDataForModel_test, file = "~/tmp/dfDataForModel_test.RDS")

folds <- createFolds(dfDataForModel_train$ad_id, k = 5, list = TRUE, returnTrain = TRUE)

f <- 0


main_predictors <-
  c("make")

predictors <-
  c("log_mileage", "log_age", "model", "engine_power", "fuel_type",
  "body_type", "gearbox", "engine_capacity", "fuel_type", "nr_seats"
    )

for(predictors_num in 1:length(predictors)){

selected_predictors <- c(main_predictors, head(predictors, predictors_num))

formula <-
  paste("price_PLN~", paste(selected_predictors, collapse="+"))
print(formula)

model_formula <- as.formula(formula)



for(i in folds){
  f <- f +1
dfInputForModel_train <- dfDataForModel_train[i, ]
dfInputForModel_cv <- dfDataForModel_train[-i, ]

learning_sample_size_train <- nrow(dfInputForModel_train)
learning_sample_size_cv <- nrow(dfInputForModel_cv)
learning_sample_size_test <- nrow(dfDataForModel_test)


dfInputForModel_train <-
  dfInputForModel_train %>%
  arrange(make, model, year, age, body_type, engine_capacity, engine_power,
          gearbox, nr_seats, new_used, fuel_type)

dfInputForModel_cv <-
  dfInputForModel_cv %>%
  arrange(make, model, year, age, body_type, engine_capacity, engine_power,
          gearbox, nr_seats, new_used, fuel_type)

#save(object = dfInputForModel_train, file = "~/tmp/dfInputForModel_train.RDS")
#save(object = dfInputForModel_cv, file = "~/tmp/dfInputForModel_cv.RDS")

for (iNtrees in c(100, 200, 300, 400, 500, 1000)){
for (iMtry in seq(1, length(selected_predictors)-1, 1)){
  
a <- NULL
aT <- NULL

t=Sys.time()

# kilometer, age of vehicle, power of vehicle and brand

print(paste(f, iNtrees, iMtry))
RF_model <-
  ranger(formula = model_formula,
         data =  dfInputForModel_train,
         num.trees = iNtrees, num.threads = 8, verbose = TRUE, 
         mtry = iMtry, importance = "impurity", 
         min.node.size = 5
  )

print(formula)
print(paste("Trees", iNtrees, "Mtry", iMtry))
dfInputForModel_train$predictedPrice_PLN <- RF_model$predictions

# Calculate mean error, mean absolute error, mean squared error, etc.
a <- gof(dfInputForModel_train$predictedPrice_PLN, dfInputForModel_train$price_PLN)

a <- data.frame(dataset = "train",
                formula = formula, 
                num.independent.variables = RF_model$num.independent.variables,
                #variable.importance = as.list(RF_model$variable.importance),
                prediction.error = RF_model$prediction.error,
                r.squared = RF_model$r.squared,
                learning_sample_size_train = learning_sample_size_train,
                learning_sample_size_cv = learning_sample_size_cv,
                learning_sample_size_test = learning_sample_size_test,
                ntrees = iNtrees, 
                mtry = iMtry, 
                resultsName = row.names(a),
                resultsValue = a[,1],
                kfold = f
                )

e <-
  a %>%
  spread(resultsName, resultsValue) %>%
  arrange(MAE, MSE,RMSE)

saveRDS(object = e, 
        file = paste0("~/tmp/", id, "model_results_train_stats_",
                      iNtrees,"_",iMtry,"_", f, "_", ".RDS"))

print("Train Results")
#print(a)




# Predict price using TRAIN folds from cross validation dataset ----------

p <- predict(RF_model, data = dfInputForModel_cv, num.threads = 7)

dfInputForModel_cv$predictedPrice_PLN <- p$predictions


aT <- gof(dfInputForModel_cv$predictedPrice_PLN, dfInputForModel_cv$price_PLN)

aT <- data.frame(dataset = "cv",
                 formula = formula, 
                 num.independent.variables = RF_model$num.independent.variables,
                 #variable.importance = as.list(RF_model$variable.importance),
                 prediction.error = RF_model$prediction.error,
                 r.squared = RF_model$r.squared,
                 learning_sample_size_train = learning_sample_size_train,
                 learning_sample_size_cv = learning_sample_size_cv,
                learning_sample_size_test = learning_sample_size_test,
                ntrees = iNtrees, 
                mtry = iMtry, 
                resultsName = row.names(aT),
                resultsValue = aT[ ,1],
                kfold = f
)


eT <-
  aT %>%
  spread(resultsName, resultsValue) %>%
  arrange(MAE, MSE,RMSE)

saveRDS(object = eT, 
        file = paste0("~/tmp/", id, "model_results_cv_stats_",
                      iNtrees,"_",iMtry,"_", f, "_", ".RDS"))

print("CV Results")
#print(aT)



# Predict price using TEST dataset ----------------------------------------

p <- predict(RF_model, data = dfDataForModel_test, num.threads = 8)

dfDataForModel_test$predictedPrice_PLN <- p$predictions


aTT <- gof(dfDataForModel_test$predictedPrice_PLN, dfDataForModel_test$price_PLN)

aTT <- data.frame(dataset = "test",
                  formula = formula, 
                  num.independent.variables = RF_model$num.independent.variables,
                  #variable.importance = as.list(RF_model$variable.importance),
                  prediction.error = RF_model$prediction.error,
                  r.squared = RF_model$r.squared,
                 learning_sample_size_train = learning_sample_size_train,
                 learning_sample_size_cv = learning_sample_size_cv,
                 learning_sample_size_test = learning_sample_size_test,
                 ntrees = iNtrees, 
                 mtry = iMtry, 
                 resultsName = row.names(aTT),
                 resultsValue = aTT[ ,1],
                 kfold = f
)


eTT <-
  aTT %>%
  spread(resultsName, resultsValue) %>%
  arrange(MAE, MSE,RMSE)

saveRDS(object = eTT, 
        file = paste0("~/tmp/", id, "model_results_test_stats_",
                      iNtrees,"_",iMtry,"_", f, "_", ".RDS"))

print("Test Results")
#print(aTT)

rm(RF_model)
gc()
}
}
}
}
#}

print("The End")
Sys.time()
