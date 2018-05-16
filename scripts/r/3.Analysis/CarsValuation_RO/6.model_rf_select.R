#'
#' CPP Romania
#' Select best RF model from all simulations
#' 
#' Load xgboost table with model simulations and compare performance
#' 
#' To compare performance: average error %, MAE over different folds
#' 
#' model simulations have been previously generated results exported to "param_grid_cv_xgboost.csv" data frame


# setwd("~/Verticals-bi/scripts/r/3.Analysis/CarsValuation_RO")


# Load tables with model simulations
param_grid <- read_csv("rf_data/rf_sim_2018-05-11.csv")


# Find optimal combination of hyperparameters that minimize error (without averaging over folds)
opt_index <- which.min(param_grid[, "error_per"]) 
opt_hyp <- param_grid[opt_index,]   # ~= 15.8%
opt_hyp

opt_index2 <- which.min(param_grid[, "MAE"]) 
opt_hyp2 <- param_grid[opt_index,]   # ~= 1600 eur => the same parameters combination have min value on both errors
opt_hyp2

# Now average error over folds
param_grid_cv<- param_grid %>%
  group_by(formula, n_tree) %>%
  summarize(mean_error_per= mean(error_per), mean_MAE=mean(MAE))
View(param_grid_cv)
param_grid_cv <- as.data.frame(param_grid_cv)

opt_index_cv <- which.min(param_grid_cv[, "mean_error_per"]) 
opt_hyp_cv <- param_grid_cv[opt_index_cv,] 
opt_hyp_cv   # ~= 16.42%

#' Conclusions:
#' Best performance was found on folder 5 with 15.8% error (max number of predictors and 500 trees). 
#' However when averaging error over all 5 folds, formulas with 1000 trees perform slighly better.
#' 
#' Will choose a model with 500 trees (and all 9 parameters) mainly because the difference in performance is very small
#' and a very large number trees might lead to overfitting?


