#Date of execution
OtomotoExecutedDate <- Sys.Date()

#Define the ID to Otomoto project in GA
ids <- "ga:5485250"

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)
# Load up the Scales package.
library('scales')

# Authorize the Google Analytics account
# ga_token <- authorize(username = Clientusername,
#           client.id = ClientId,
#           client.secret = ClientSecret,
#           cache = TRUE, reauth = FALSE, token = NULL)
# 
# save(ga_token, file = "tokenOtomoto.RData")

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

# Perform query and assign the results to a "data frame" called gaDataTotalSortingOtomoto
gaDataTotalSortingOtomoto <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                 end.date = "yesterday", metrics = c("ga:pageViews"), 
                 dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@.order.=filter_float",
                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                 max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))

# Change the Columns name
colnames(gaDataTotalSortingOtomoto) <- c("Date","Total Sorting")

# Perform query and assign the results to a "data frame" called gaDataKmSortingOtomoto
gaDataKmSortingOtomoto <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                             end.date = "yesterday", metrics = c("ga:pageViews"), 
                             dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@.order.=filter_float_mileage",
                             segment = NULL, samplingLevel = NULL, start.index = NULL,
                             max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))


# Change the Columns name
colnames(gaDataKmSortingOtomoto) <- c("Date","KM Sorting")

# Perform query and assign the results to a "data frame" called gaDataEPSortingOtomoto
gaDataEPSortingOtomoto <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                          end.date = "yesterday", metrics = c("ga:pageViews"), 
                          dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@.order.=filter_float_engine_power",
                          segment = NULL, samplingLevel = NULL, start.index = NULL,
                          max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))


# Change the Columns name
colnames(gaDataEPSortingOtomoto) <- c("Date","PE Sorting")

# merge two data frames by Date
TotalOtomoto <- merge(gaDataTotalSortingOtomoto,gaDataKmSortingOtomoto,by="Date")
# merge two data frames by Date
TotalOtomoto <- merge(TotalOtomoto,gaDataEPSortingOtomoto,by="Date")

#Convert Date to Character
# TotalOtomoto[,1] <- as.character(TotalOtomoto[,1])
# 
# #Create Total Line
# TotalOtomoto <- rbind(TotalOtomoto, c("Total", colSums (TotalOtomoto[,c(2,3,4)])))
# 
# #Convert the metrics to integer
# TotalOtomoto[,2] <- as.integer(TotalOtomoto[,2])
# TotalOtomoto[,3] <- as.integer(TotalOtomoto[,3])
# TotalOtomoto[,4] <- as.integer(TotalOtomoto[,4])

# Calculate the percentage of sorting usage
TotalOtomoto$"KM Sorting %" <- percent(round(TotalOtomoto$"KM Sorting"/TotalOtomoto$"Total Sorting",4))

# Calculate the percentage of sorting usage
TotalOtomoto$"PE Sorting %" <- percent(round(TotalOtomoto$"PE Sorting"/TotalOtomoto$"Total Sorting",4))

# Change the ordem of exebition
ExibitionOtomoto <- TotalOtomoto[,c("Date",
                     "KM Sorting",
                     "KM Sorting %",
                     "PE Sorting",
                     "PE Sorting %",
                     "Total Sorting")]

#Save the Dataframe ExibitionOtomoto in a file to have Cache
save(ExibitionOtomoto,OtomotoExecutedDate,TotalOtomoto, file = "ExibitionOtomoto.RData")

