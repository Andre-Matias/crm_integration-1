library("ranger")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("hydroGOF")
library("tidyr")
library("caret")
library("parallel")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

#clear garbage
rm(list=setdiff(ls(), c("myS3key","MyS3SecretAccessKey")))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

#config
origin_bucket_path <- "s3://pyrates-data-ocean/"
origin_bucket_prefix <- "datalake/autovitRO/AIO/"
vertical <- "autovitRO"
tmp_dir <- "~/tmp/"
id <- as.character(as.hexmode(as.integer(Sys.time())))

tmp_dir <- paste0(tmp_dir, "AQS_", format(Sys.time(), "%Y%m%d_%H%M%S/"))

dir.create(tmp_dir)

dfInputToModel <-
  as_tibble(
    s3readRDS(object = paste0(origin_bucket_prefix, "dfInputToModel_AQS.RDS"), bucket = origin_bucket_path)
  )

#dfInputToModel <- head(dfInputToModel, 1000)

dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$mileage), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$year), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$model), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$engine_power), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$fuel_type), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$body_type), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$gearbox), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$engine_capacity), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$priceValue), ]
dfInputToModel <- dfInputToModel[!is.na(dfInputToModel$nr_images), ]

target <- "qtyMessagesOnAtlas_7"

predictors <-
  c("mileage", "year", "model", "engine_power", "fuel_type",
    "body_type", "gearbox", "engine_capacity", "priceValue", "nr_images"
  )

test_idx <- sample(x = nrow(dfInputToModel), size = (1 - 1/6) * nrow(dfInputToModel))

dfDataForModel_train <- dfInputToModel[test_idx, ]
dfDataForModel_test <- dfInputToModel[-test_idx, ]

folds <- createFolds(dfDataForModel_train$ad_id, k = 5, list = TRUE, returnTrain = TRUE)

f <- 0

main_predictors <-
  c("make")

predictors <-
  c("mileage", "year", "model", "engine_power", "fuel_type",
    "body_type", "gearbox", "engine_capacity", "priceValue", "nr_images"
  )

for(predictors_num in 1:length(predictors)){
  
  selected_predictors <- c(main_predictors, head(predictors, predictors_num))
  
  formula <-
    paste(target, "~", paste(selected_predictors, collapse="+"))
  print(formula)
  
  model_formula <- as.formula(formula)
  
  for(i in folds){
    f <- f +1
    dfInputForModel_train <- dfDataForModel_train[i, ]
    dfInputForModel_cv <- dfDataForModel_train[-i, ]
    
    learning_sample_size_train <- nrow(dfInputForModel_train)
    learning_sample_size_cv <- nrow(dfInputForModel_cv)
    learning_sample_size_test <- nrow(dfDataForModel_test)

    # dfInputForModel_train <-
    #   dfInputForModel_train %>%
    #   arrange(make, model, year, age, body_type, engine_capacity, engine_power,
    #           gearbox, nr_seats, new_used, fuel_type)
    # 
    # dfInputForModel_cv <-
    #   dfInputForModel_cv %>%
    #   arrange(make, model, year, age, body_type, engine_capacity, engine_power,
    #           gearbox, nr_seats, new_used, fuel_type)
    
    #save(object = dfInputForModel_train, file = "~/tmp/dfInputForModel_train.RDS")
    #save(object = dfInputForModel_cv, file = "~/tmp/dfInputForModel_cv.RDS")
    
    for (iNtrees in c(100, 200, 300, 400, 500, 1000)){
      #for (iMtry in seq(1, length(selected_predictors)-1, 1)){
      for (iMtry in ceiling(sqrt(length(selected_predictors)))){
        
        a <- NULL
        aT <- NULL
        
        t=Sys.time()
        
        print(paste(f, iNtrees, iMtry))
        RF_model <-
          ranger(formula = model_formula,
                 data =  dfInputForModel_train,
                 num.trees = iNtrees, num.threads = detectCores()-1 , verbose = TRUE, 
                 mtry = iMtry, importance = "impurity", 
                 min.node.size = 5
          )
        
        print(formula)
        print(paste("Trees", iNtrees, "Mtry", iMtry))
        dfInputForModel_train$predictedqtyAdImpressions_7 <- RF_model$predictions
        
        # Calculate mean error, mean absolute error, mean squared error, etc.
        a <- gof(dfInputForModel_train$predictedqtyAdImpressions_7, dfInputForModel_train$qtyAdImpressions_7)
        
        a <- data.frame(dataset = "train",
                        formula = formula, 
                        num.independent.variables = RF_model$num.independent.variables,
                        variable.importance = as.list(RF_model$variable.importance),
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
                file = paste0(tmp_dir, id, "model_results_train_stats_",
                              iNtrees,"_",iMtry,"_", f, "_", ".RDS"))
        
        print("Train Results")
        
        p <- predict(RF_model, data = dfInputForModel_cv, num.threads = detectCores()-1)
        
        dfInputForModel_cv$predictedqtyAdImpressions_7 <- p$predictions
        
        
        aT <- gof(dfInputForModel_cv$predictedqtyAdImpressions_7, dfInputForModel_cv$qtyAdImpressions_7)
        
        aT <- data.frame(dataset = "cv",
                         formula = formula, 
                         num.independent.variables = RF_model$num.independent.variables,
                         variable.importance = as.list(RF_model$variable.importance),
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
                file = paste0(tmp_dir, id, "model_results_cv_stats_",
                              iNtrees,"_",iMtry,"_", f, "_", ".RDS"))
        
        print("CV Results")
        
        p <- predict(RF_model, data = dfDataForModel_test, num.threads = 8)
        
        dfDataForModel_test$predictedqtyAdImpressions_7 <- p$predictions
        
        
        aTT <- gof(dfDataForModel_test$predictedqtyAdImpressions_7, dfDataForModel_test$qtyAdImpressions_7)
        
        aTT <- data.frame(dataset = "test",
                          formula = formula, 
                          num.independent.variables = RF_model$num.independent.variables,
                          variable.importance = as.list(RF_model$variable.importance),
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
                file = paste0(tmp_dir, id, "model_results_test_stats_",
                              iNtrees,"_",iMtry,"_", f, "_", ".RDS"))
        
        print("Test Results")

        rm(RF_model)
        gc()
      }
    }
  }
}
#}

print("The End")

print(paste("Results:", tmp_dir))
Sys.time()
