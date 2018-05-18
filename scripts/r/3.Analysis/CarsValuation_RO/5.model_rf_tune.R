#' CPP Romania
#' Build several Random Forest models with different combinations hyperparameters using the training set
#' 
#' 1) Split dataset between training and test set
#' 2) Subsplit the training test for k-fold cross-validation
#' 3) Generate an expand grid with several combinations of RF hyperparameters to be simulated 
#' 4) For each kfold and combination of hyperparameters 
#'    - build a model
#'    - predict new values on k-1 fold
#'    - evaluate error (using MAE, error %, R squared)
#'    
#' Final output: an expand grid with error for each k-fold and hyperparameters combination simulated
#' 



library("ranger")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("hydroGOF")
library("tidyr")
library("caret")

id <- as.character(as.hexmode(as.integer(Sys.time())))

# Load dataset ready for modeling ---------------------------------------------
dfDataForModel <- read_csv("dfDataForModel.csv")
  
# Split into training and test set: -------------------------------------------
## 80% for training and 20% for test as for RF
set.seed(123)
test_idx <- sample(x = nrow(dfDataForModel), size = (1 - 1/6) * nrow(dfDataForModel))

dfDataForModel_train <- dfDataForModel[test_idx, ]
dfDataForModel_test <- dfDataForModel[-test_idx, ]

# Subsplit training for cross-validation --------------------------------------
folds <- createFolds(dfDataForModel_train$ad_id, k = 5, list = TRUE, returnTrain = TRUE)


## Define of possible values for minsplit and maxdepth

formula <- c("price_RON~ make+mileage", 
             "price_RON~ make+mileage+age", 
             "price_RON~ make+mileage+age+model",
             "price_RON~ make+mileage+age+model+engine_power", 
             "price_RON~ make+mileage+age+model+engine_power+fuel_type",
             "price_RON~ make+mileage+age+model+engine_power+fuel_type+body_type", 
             "price_RON~ make+mileage+age+model+engine_power+fuel_type+body_type+gearbox",
             "price_RON~ make+mileage+age+model+engine_power+fuel_type+body_type+gearbox+engine_capacity"
             )

n_tree <- c(100, 200, 300, 500, 1000)
n_folds <- seq(1, 5, 1)

## Create a data frame containing all combinations 
param_grid <- expand.grid( formula = formula, n_tree= n_tree, n_folds = n_folds)

## Check out the grid
head(param_grid)

## Number of potential models in the grid
num_models <- nrow(param_grid)
num_models

## Initiate columns where we will store error metrics
param_grid$error_per <-NA
param_grid$MAE <- NA
param_grid$n_predictors <- rep(2:9, length(n_tree) * length(n_folds))
#param_grid$Rsquared <- NA
head(param_grid)

# Loop over the rows of param_grid to: ----------------------------------------
## 1) train the grid of models
## 2) predict on validation set
## 3) calculate error
## 4) store error in param_grid table


for (i in 1:num_models) {
  Sys.time()
  
  # Get hyperparameters values at row i
  model_formula <- as.formula(as.character(param_grid$formula[i]))
  print(paste("model_formula:", " ", model_formula))
  n_tree <- param_grid$n_tree[i]
  print(paste("n_tree:", " ", n_tree))
  n_folds <- param_grid$n_folds[i]
  print(paste("n_folds:", " ", n_folds))
  
  
  # Train a model
  RF_model <-
    ranger(formula = model_formula, 
           data =  dfDataForModel_train[folds[[n_folds]], ],
           num.trees = n_tree, num.threads = 8, verbose = TRUE, 
           importance = "impurity", 
           min.node.size = 5
    )

  
  
  # Predict price using validation set 
  val_set <- dfDataForModel_train[-folds[[n_folds]], ]
  
  pred <- predict(RF_model, val_set)
  val_set$pred_price <- pred$predictions
  

  # Calculate error
  ## MAE: mean absolute error
  val_set$error_abs <- 
    abs(val_set$price_RON - val_set$pred_price)
  
  param_grid$MAE[i] <- mean(val_set$error_abs)
  
  
  ## error_per: mean of error % over the price posted in the ad
  val_set$error_per <-
    val_set$error_abs / val_set$price_RON
  param_grid$error_per[i] <- mean(val_set$error_per)
  
  paste ("model number","", print(i), "of", "", num_models)
  print(head(param_grid, i))
  
  # Clean workspace
  rm(RF_model, val_set, pred)
  Sys.time()
  
}






# model_formula <- as.formula(as.character(param_grid$formula[1]))
# RF_model <-
#   ranger(formula = model_formula, 
#          data =  sample_n(dfDataForModel_train, 500),
#          num.trees = 100, num.threads = 8, verbose = TRUE, 
#         importance = "impurity", 
#          min.node.size = 5
#   )


# Save param_grid with RF simulations
write.csv(param_grid, file= paste0("rf_data/rf_sim_", Sys.Date(), ".csv"))






# Generate expand grid --------------------------------------------------------

# main_predictors <-
#   c("make")
# 
# predictors <-
#   c("mileage", "age", "model", "engine_power", "fuel_type",
#     "body_type", "gearbox", "engine_capacity"
#   )
# 
# formulas <- character(10)
# for(predictors_num in 1:length(predictors)){
#   
#   selected_predictors <- c(main_predictors, head(predictors, predictors_num))
#   
#   formula <-
#     paste("price_RON~", paste(selected_predictors, collapse="+"))
#   print(formula)
#   
#   #model_formula <- as.formula(formula)
#   formulas[i] <- formula
# }
