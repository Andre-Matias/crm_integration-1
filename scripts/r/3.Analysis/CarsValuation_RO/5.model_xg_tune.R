
#' CPP Romania
#' Build several xgb (extrem gradient boosting) models 
#' with different combinations hyperparameters using the training set
#' 
#' tuning:
#' - maxdepth
#' - nrounds
#' - will use best formula from RF, which includes all 9 predictors in the dataset
#' 
#' 1) Split dataset between training and test set
#' 2) Subsplit the training test for cross-validation (will do 80/20 not kfold cv)
#' 3) Generate an expand grid with several combinations of RF hyperparameters to be simulated 
#' 4) For each combination of hyperparameters 
#'    - build a model on 80% of training set
#'    - predict new values on 20% of training set
#'    - evaluate error (using MAE, error %)
#'    
#' Final output: an expand grid with error for hyperparameters combination simulated
#' 


setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")

library("xgboost")
library("Matrix")
library("tidyverse")


# Load dataset ready for modeling ---------------------------------------------
dfDataForModel <- read_csv("datasets/dfDataForModel.csv") %>% 
  select (-ad_id, -year)
glimpse(dfDataForModel)


# One-hot encoding 
## to transform categorical variables into a binary featuresparse.model -------
categorical_matrix <- model.matrix(~make+model+body_type+gearbox+fuel_type -1, dfDataForModel)
df_matrix <- dfDataForModel %>%
  select(mileage, engine_capacity, engine_power, age, price_RON) %>%
  as.matrix()
df_matrix <- cbind(df_matrix, categorical_matrix)


# Split into training and test set: ------------------------------------------
## 80% for training and 20% for test as for RF
set.seed(123)
test_idx <- sample(x = nrow(df_matrix), size = (1 - 1/6) * nrow(df_matrix))

dfDataForModel_train_xgb <- df_matrix[test_idx, ]
dfDataForModel_test_xgb <- df_matrix[-test_idx, ]

## separate between data and labels
train_data <- subset(dfDataForModel_train_xgb, select= -price_RON)
train_label <- subset(dfDataForModel_train_xgb, select= price_RON)

test_data <- subset(dfDataForModel_test_xgb, select= -price_RON)
test_label <- subset(dfDataForModel_test_xgb, select= price_RON)

## Convert matrixes into a dmatrix object 
## (help in training performance and is necessary if we wanna use multiple cores)
d_train <- xgb.DMatrix(data = train_data, label= train_label)
d_test <- xgb.DMatrix(data = test_data, label= test_label)

# Clean workspace
rm(categorical_matrix, df_matrix)


# Subsplit training for cross-validation --------------------------------------
## 80% for training and 20% for validation
set.seed(444)
cv_idx <- sample(x = nrow(dfDataForModel_train_xgb), size = (1 - 1/6) * nrow(dfDataForModel_train_xgb))

cv_train <- dfDataForModel_train_xgb[cv_idx, ]
cv_validate <- dfDataForModel_train_xgb[-cv_idx, ]

## separate between data and labels
cv_train_data <- subset(cv_train, select= -price_RON)
cv_train_label <- subset(cv_train, select= price_RON)

cv_validate_data <- subset(cv_validate, select= -price_RON)
cv_validate_label <- subset(cv_validate, select= price_RON)

## Convert matrixes into a dmatrix object 
## (helps in training performance and is necessary if we wanna use multiple cores)
cv_train <- xgb.DMatrix(data = cv_train_data, label= cv_train_label)
cv_validate <- xgb.DMatrix(data = cv_validate_data, label= cv_validate_label)



# Hyperparameters tuning ------------------------------------------------------

## Define of possible values for minsplit and maxdepth
max_depth <- seq(3, 12, 3)
nrounds <- seq(2, 20, 2)

## Create a data frame containing all combinations 
param_grid <- expand.grid(max_depth = max_depth, nrounds = nrounds)
head(param_grid)

## Number of potential models in the grid
num_models <- nrow(param_grid)
num_models

## Initiate columns where we will store error metrics
param_grid$MAE <- NA
param_grid$error_per <- NA

## Loop over the rows of param_grid to:
## 1) train the grid of models
## 2) predict on validation set
## 3) calculate error
## 4) store error in param_grid table

for (i in 1:num_models) {
  Sys.time()
  
  # Get minsplit, maxdepth values at row i
  max_depth <- param_grid$max_depth[i]
  print(paste("max_depth:", " ", max_depth))
  nrounds <- param_grid$nrounds[i]
  print(paste("nrounds:", " ", nrounds))
  
  # Train a model
  XGB_model <- xgboost(data = cv_train,
                       max_depth = max_depth, 
                       eta = 1, 
                       nthread = 2, 
                       nrounds = nrounds, 
                       objective = "reg:linear"
                       #early_stopping_rounds = 3
  )
  
  
  
  # Predict price using validation set 
  pred <- predict(XGB_model, cv_validate)
  
  results <- as.data.frame(cbind(cv_validate_data, cv_validate_label, pred))
  
  
  # Calculate error
  
  ## MAE: mean absolute error
  results$MAE <- 
    abs(results$price_RON - results$pred)
  
  param_grid$MAE[i] <- mean(results$MAE)
  
  
  # ME %: mean of error % over the price posted in the ad
  results$error_per <-
    results$MAE / results$price_RON
  param_grid$error_per[i] <- mean(results$error_per)
  
  paste ("model number","", print(i), "of", "", num_models)
  print(head(param_grid, i))
  
  # Clean workspace
  rm(XGB_model, pred, results)
  Sys.time()
  
}



# Save param_grid with xgb simulations
write.csv(param_grid, file= paste0("xgb_data/xgb_sim_", Sys.Date(), ".csv"))
