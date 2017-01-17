#Date of execution
AutovitExecutedDate <- Sys.Date()

#Define the ID to Autovit project in GA
ids <- "ga:22130385"

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)
# Load up the Scales package.
library('scales')

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

source("function.R")

#########Total Sort######
# Perform query and assign the results to a "data frame" called gaDataTotalSortingAutovit
gaDataTotalSortingAutovit <- QueryGA(".order.=")
# Change the Columns name
colnames(gaDataTotalSortingAutovit) <- c("Date","Total Sorting")

#########Create At######

# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataCreatedAtSortingAutovit <- QueryGA(".order.=created_at")
# Change the Columns name
colnames(gaDataCreatedAtSortingAutovit) <- c("Date","Created at Sorting")

#########Price######

# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataPriceAscSortingAutovit <- QueryGA(".order.=filter_float_price:asc")
# Change the Columns name
colnames(gaDataPriceAscSortingAutovit) <- c("Date","Price Asc Sorting")


# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataPriceDescSortingAutovit <- QueryGA(".order.=filter_float_price:desc")
# Change the Columns name
colnames(gaDataPriceDescSortingAutovit) <- c("Date","Price Desc Sorting")

#########KM######

# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataKmAscSortingAutovit <- QueryGA(".order.=filter_float_mileage:asc")
# Change the Columns name
colnames(gaDataKmAscSortingAutovit) <- c("Date","KM Asc Sorting")

# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataKmDescSortingAutovit <- QueryGA(".order.=filter_float_mileage:desc")
# Change the Columns name
colnames(gaDataKmDescSortingAutovit) <- c("Date","KM Desc Sorting")

#########PE######

# Perform query and assign the results to a "data frame" called gaDataEPSortingAutovit
gaDataEPAscSortingAutovit <- QueryGA(".order.=filter_float_engine_power:asc")
# Change the Columns name
colnames(gaDataEPAscSortingAutovit) <- c("Date","PE Asc Sorting")

# Perform query and assign the results to a "data frame" called gaDataEPSortingAutovit
gaDataEPDescSortingAutovit <- QueryGA(".order.=filter_float_engine_power:desc")
# Change the Columns name
colnames(gaDataEPDescSortingAutovit) <- c("Date","PE Desc Sorting")



##########Merge DataFrames###########
# merge two data frames by Date
TotalAutovit <- merge(gaDataTotalSortingAutovit,gaDataCreatedAtSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataPriceAscSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataPriceDescSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataKmAscSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataKmDescSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataEPAscSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataEPDescSortingAutovit,by="Date")

##########Calculate Sum and Percentage###########
# Calculate the percentage of sorting usage
TotalAutovit$"Price Sorting" <- TotalAutovit$"Price Asc Sorting"+TotalAutovit$"Price Desc Sorting"
# Calculate the percentage of sorting usage
TotalAutovit$"KM Sorting" <- TotalAutovit$"KM Asc Sorting"+TotalAutovit$"KM Desc Sorting"
# Calculate the percentage of sorting usage
TotalAutovit$"PE Sorting" <- TotalAutovit$"PE Asc Sorting"+TotalAutovit$"PE Desc Sorting"
# Calculate the percentage of sorting usage
TotalAutovit$"Created at Sorting %" <- percent(round(TotalAutovit$"Created at Sorting"/TotalAutovit$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalAutovit$"Price Sorting %" <- percent(round(TotalAutovit$"Price Sorting"/TotalAutovit$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalAutovit$"KM Sorting %" <- percent(round(TotalAutovit$"KM Sorting"/TotalAutovit$"Total Sorting",4))
# Calculate the percentage of sorting usage
TotalAutovit$"PE Sorting %" <- percent(round(TotalAutovit$"PE Sorting"/TotalAutovit$"Total Sorting",4))

#######################
# Change the ordem of exebition
ExibitionAutovit <- TotalAutovit[,c("Date",
                                    "Price Sorting",
                                    "Price Sorting %",
                                    "Created at Sorting",
                                    "Created at Sorting %",
                                    "KM Sorting",
                                    "KM Sorting %",
                                    "PE Sorting",
                                    "PE Sorting %",
                                    "Total Sorting")]

#Save the Dataframe ExibitionAutovit in a file to have Cache
save(ExibitionAutovit,AutovitExecutedDate,TotalAutovit, file = "ExibitionAutovit.RData")

