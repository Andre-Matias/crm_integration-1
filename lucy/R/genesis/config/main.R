# libraries
library(googlesheets)
library(dplyr)
library(jsonlite)
library(RMixpanel)
library(RPostgreSQL)
library(plyr)

# Dates
from_date <- as.character(Sys.Date()-1)
to_date <-  as.character(Sys.Date()-1)
