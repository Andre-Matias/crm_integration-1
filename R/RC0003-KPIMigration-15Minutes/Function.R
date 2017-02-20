QueryGA <- function(dimensionsVariable,filterVariable){
  data.frame(get_ga(profileId = ids, start.date = filterVariable,
                                                 end.date = "yesterday", metrics = c("ga:sessions","ga:pageViews","ga:bounceRate","ga:users"), 
                                                 dimensions = dimensionsVariable, sort = NULL, filters = NULL,
                                                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                                                 max.results = NULL, include.empty.rows = NULL, fetch.by = "day", ga_token))
  
  
}

QueryATI <- function(urlRestAPI){

  rawDataFrameFromRestAPI <- getURL(urlRestAPI)
  
  #transform JSON string into a data.frame 
  rawDataFrameFromJSON <- fromJSON(rawDataFrameFromRestAPI)
  
  rawDataFrameFromList <- rawDataFrameFromJSON$DataFeed$Rows
  
  rawDataFrameFromList <- as.data.frame(rawDataFrameFromList)
  
}