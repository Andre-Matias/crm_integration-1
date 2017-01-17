


QueryGA <- function(filterVaraiable){
  gaDataTotalSortingAutovit <- data.frame(get_ga(profileId = ids, start.date = "2016-12-12",
                                                 end.date = "yesterday", metrics = c("ga:pageViews"), 
                                                 dimensions = c("ga:date"), sort = NULL, filters = paste("ga:pagePath=@",filterVaraiable, sep = ""),
                                                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                                                 max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))
  

}