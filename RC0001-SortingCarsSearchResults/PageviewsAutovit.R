#Date of execution
AutovitExecutedDate <- Sys.Date()

#Define the ID to Autovit project in GA
ids <- "ga:22130385"

# Load up the RGA package. 
# connect to and pull data from the Google Analytics API
library(RGA)

# Load up the Scales package.
library('scales')

# Authorize the Google Analytics account
# ga_token <- authorize(username = Clientusername,
#            client.id = ClientId,
#            client.secret = ClientSecret,
#            cache = TRUE, reauth = FALSE, token = NULL)
#  
#  save(ga_token, file = "tokenGA.RData")

#Load the file containing the Authorization to Access GA
load("tokenGA.RData")

# Perform query and assign the results to a "data frame" called gaDataTotalSortingAutovit
gaDataTotalSortingAutovit <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                 end.date = "yesterday", metrics = c("ga:pageViews"), 
                 dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@[order]=filter_float",
                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                 max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))

# Change the Columns name
colnames(gaDataTotalSortingAutovit) <- c("Date","Total Sorting")

# Perform query and assign the results to a "data frame" called gaDataKmSortingAutovit
gaDataKmSortingAutovit <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                             end.date = "yesterday", metrics = c("ga:pageViews"), 
                             dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@[order]=filter_float_mileage",
                             segment = NULL, samplingLevel = NULL, start.index = NULL,
                             max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))


# Change the Columns name
colnames(gaDataKmSortingAutovit) <- c("Date","KM Sorting")

# Perform query and assign the results to a "data frame" called gaDataEPSortingAutovit
gaDataEPSortingAutovit <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                          end.date = "yesterday", metrics = c("ga:pageViews"), 
                          dimensions = c("ga:date"), sort = NULL, filters = "ga:pagePath=@[order]=filter_float_engine_power",
                          segment = NULL, samplingLevel = NULL, start.index = NULL,
                          max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))


# Change the Columns name
colnames(gaDataEPSortingAutovit) <- c("Date","PE Sorting")

# merge two data frames by Date
TotalAutovit <- merge(gaDataTotalSortingAutovit,gaDataKmSortingAutovit,by="Date")
# merge two data frames by Date
TotalAutovit <- merge(TotalAutovit,gaDataEPSortingAutovit,by="Date")
#TotalAutovit[,1] <- as.Date(TotalAutovit[,1])
# #Convert Date to Character
# TotalAutovit[,1] <- as.character(TotalAutovit[,1])
# 
# #Create Total Line
# TotalAutovit <- rbind(TotalAutovit, c("Total", colSums (TotalAutovit[,c(2,3,4)])))
# 
# #Convert the metrics to integer
# TotalAutovit[,2] <- as.integer(TotalAutovit[,2])
# TotalAutovit[,3] <- as.integer(TotalAutovit[,3])
# TotalAutovit[,4] <- as.integer(TotalAutovit[,4])

# Calculate the percentage of sorting usage
TotalAutovit$"KM Sorting %" <- percent(round(TotalAutovit$"KM Sorting"/TotalAutovit$"Total Sorting",4))

# Calculate the percentage of sorting usage
TotalAutovit$"PE Sorting %" <- percent(round(TotalAutovit$"PE Sorting"/TotalAutovit$"Total Sorting",4))

# Change the ordem of exebition
ExibitionAutovit <- TotalAutovit[,c("Date",
                     "KM Sorting",
                     "KM Sorting %",
                     "PE Sorting",
                     "PE Sorting %",
                     "Total Sorting")]

#Save the Dataframe ExibitionAutovit in a file to have Cache
save(ExibitionAutovit,AutovitExecutedDate,TotalAutovit, file = "ExibitionAutovit.RData")

