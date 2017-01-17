#Date of execution
OtomotoExecutedDate <- Sys.Date()

#Define the ID to Otomoto project in GA
ids <- "ga:5485250"

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)
# Load up the Scales package.
library('scales')

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

source("function.R")

#########Total Sort######
# Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
gaDataTotalSortingOtomoto <- QueryGA(".order.=")
# Change the Columns name
colnames(gaDataTotalSortingOtomoto) <- c("Date","Total Sorting")

#########Create At######

# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataCreatedAtSortingOtomoto <- QueryGA(".order.=created_at")
# Change the Columns name
colnames(gaDataCreatedAtSortingOtomoto) <- c("Date","Created at Sorting")

#########Price###########

# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataPriceAscSortingOtomoto <- QueryGA(".order.=filter_float_price:asc")
# Change the Columns name
colnames(gaDataPriceAscSortingOtomoto) <- c("Date","Price Asc Sorting")


# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataPriceDescSortingOtomoto <- QueryGA(".order.=filter_float_price:desc")
# Change the Columns name
colnames(gaDataPriceDescSortingOtomoto) <- c("Date","Price Desc Sorting")

#########KM###########
# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataKmAscSortingOtomoto <- QueryGA(".order.=filter_float_mileage:asc")
# Change the Columns name
colnames(gaDataKmAscSortingOtomoto) <- c("Date","KM Asc Sorting")

# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataKmDescSortingOtomoto <- QueryGA(".order.=filter_float_mileage:desc")
# Change the Columns name
colnames(gaDataKmDescSortingOtomoto) <- c("Date","KM Desc Sorting")


#########PE##########

# Perform query and assign the results to a "data frame" called gaDataEPSortingOtomoto
gaDataEPAscSortingOtomoto <- QueryGA(".order.=filter_float_engine_power:asc")
# Change the Columns name
colnames(gaDataEPAscSortingOtomoto) <- c("Date","PE Asc Sorting")

# Perform query and assign the results to a "data frame" called gaDataEPSortingOtomoto
gaDataEPDescSortingOtomoto <- QueryGA(".order.=filter_float_engine_power:desc")
# Change the Columns name
colnames(gaDataEPDescSortingOtomoto) <- c("Date","PE Desc Sorting")


##########Merge DataFrames###########
# merge two data frames by Date
TotalOtomoto <- merge(gaDataTotalSortingOtomoto,gaDataCreatedAtSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataPriceAscSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataPriceDescSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataKmAscSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataKmDescSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataEPAscSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataEPDescSortingOtomoto,by="Date")

##########Calculate Sum and Percentage###########
# Calculate the percentage of sorting usage
TotalOtomoto$"Price Sorting" <- TotalOtomoto$"Price Asc Sorting"+TotalOtomoto$"Price Desc Sorting"
# Calculate the percentage of sorting usage
TotalOtomoto$"KM Sorting" <- TotalOtomoto$"KM Asc Sorting"+TotalOtomoto$"KM Desc Sorting"
# Calculate the percentage of sorting usage
TotalOtomoto$"PE Sorting" <- TotalOtomoto$"PE Asc Sorting"+TotalOtomoto$"PE Desc Sorting"
# Calculate the percentage of sorting usage
TotalOtomoto$"Created at Sorting %" <- percent(round(TotalOtomoto$"Created at Sorting"/TotalOtomoto$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalOtomoto$"Price Sorting %" <- percent(round(TotalOtomoto$"Price Sorting"/TotalOtomoto$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalOtomoto$"KM Sorting %" <- percent(round(TotalOtomoto$"KM Sorting"/TotalOtomoto$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalOtomoto$"PE Sorting %" <- percent(round(TotalOtomoto$"PE Sorting"/TotalOtomoto$"Total Sorting",4))


#######################
# Change the ordem of exebition
ExibitionOtomoto <- TotalOtomoto[,c("Date",
                                    "Price Sorting",
                                    "Price Sorting %",
                                    "Created at Sorting",
                                    "Created at Sorting %",
                                    "KM Sorting",
                                    "KM Sorting %",
                                    "PE Sorting",
                                    "PE Sorting %",
                                    "Total Sorting")]

#Save the Dataframe ExibitionOtomoto in a file to have Cache
save(ExibitionOtomoto,OtomotoExecutedDate,TotalOtomoto, file = "ExibitionOtomoto.RData")

