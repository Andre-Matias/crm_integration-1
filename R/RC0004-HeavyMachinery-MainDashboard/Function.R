QueryGA <- function(dimensionsVariable,filterVariable){
  data.frame(get_ga(profileId = ids, start.date = filterVariable,
                                                 end.date = "yesterday", metrics = c("ga:sessions","ga:pageViews","ga:bounceRate","ga:users","ga:bounces"), 
                                                 dimensions = dimensionsVariable, sort = NULL, filters = NULL,
                                                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                                                 max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_token))
  
  
}