QueryGA <- function(dimensionsVariable,filterVariable){
  data.frame(get_ga(profileId = ids, start.date = filterVariable,
                                                 end.date = "yesterday", metrics = c("ga:sessions","ga:pageViews","ga:bounces","ga:users"), 
                                                 dimensions = dimensionsVariable, sort = NULL, filters = NULL,
                                                 segment = NULL, samplingLevel = NULL, start.index = NULL,
                                                 max.results = NULL, include.empty.rows = NULL, fetch.by = NULL, ga_token))
  
  
}

QueryATI <- function(initialDate, endDate){
  #Load Rcurl package
  library("bitops")
  #Load Rcurl package
  library("RCurl")
  #load packge
  library("jsonlite")
  
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_visits,m_page_loads,m_bounces,m_entering_visits,m_vu}",
                       "&sort={-m_visits}",
                       "&space={s:566290}",
                       "&period={D:{start:'",initialDate,"',end:'",endDate,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=40dfd139-d477-4a72-829d-e64c7a582692"
  )
  rawDataFrameFromRestAPI <- getURL(urlRestAPI)
  
  #transform JSON string into a data.frame 
  rawDataFrameFromJSON <- fromJSON(rawDataFrameFromRestAPI)
  
  rawDataFrameFromList <- rawDataFrameFromJSON$DataFeed$Rows
  
  rawDataFrameFromList <- as.data.frame(rawDataFrameFromList)
  
}

QueryATIMonth <- function(initialDate, endDate){
  #Load Rcurl package
  library("bitops")
  #Load Rcurl package
  library("RCurl")
  #load packge
  library("jsonlite")

    # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_month,d_time_year,m_visits,m_page_loads,m_bounces,m_entering_visits,m_vu}",
                       "&sort={-m_visits}&space={s:566290}",
                       "&period={D:{start:'",initialDate,"',end:'",endDate,"'}}",
                       "&max-results=50",
                       "&page-num=1",
                       "&apikey=40dfd139-d477-4a72-829d-e64c7a582692"
  )
  rawDataFrameFromRestAPI <- getURL(urlRestAPI)
  
  #transform JSON string into a data.frame 
  rawDataFrameFromJSON <- fromJSON(rawDataFrameFromRestAPI)
  
  rawDataFrameFromList <- rawDataFrameFromJSON$DataFeed$Rows
  
  rawDataFrameFromList <- as.data.frame(rawDataFrameFromList)
  
}


QueryATIImo <- function(initialDate, endDate){
  #Load Rcurl package
  library("bitops")
  #Load Rcurl package
  library("RCurl")
  #load packge
  library("jsonlite")
  
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_date,m_visits,m_page_loads,m_bounces,m_entering_visits,m_vu}",
                       "&sort={-m_visits}",
                       "&space={s:567806}",
                       "&period={D:{start:'",initialDate,"',end:'",endDate,"'}}",
                       "&max-results=150",
                       "&page-num=1",
                       "&apikey=e87fbe6d-4fa4-4c85-9aea-73e8870b9f60"
  )
  rawDataFrameFromRestAPI <- getURL(urlRestAPI)
  
  #transform JSON string into a data.frame 
  rawDataFrameFromJSON <- fromJSON(rawDataFrameFromRestAPI)
  
  rawDataFrameFromList <- rawDataFrameFromJSON$DataFeed$Rows
  
  rawDataFrameFromList <- as.data.frame(rawDataFrameFromList)
  
}

QueryATIImoMonth <- function(initialDate, endDate){
  #Load Rcurl package
  library("bitops")
  #Load Rcurl package
  library("RCurl")
  #load packge
  library("jsonlite")
  
  # Assign Rest API to a string, concatenate with paste0()
  urlRestAPI <- paste0("https://apirest.atinternet-solutions.com/data/v2/json/getData?",
                       "&columns={d_time_month,d_time_year,m_visits,m_page_loads,m_bounces,m_entering_visits,m_vu}",
                       "&sort={-m_visits}",
                       "&space={s:567806}",
                       "&period={D:{start:'",initialDate,"',end:'",endDate,"'}}",
                       "&max-results=50",
                       "&page-num=1",
                       "&apikey=e87fbe6d-4fa4-4c85-9aea-73e8870b9f60"
  )
  rawDataFrameFromRestAPI <- getURL(urlRestAPI)
  
  #transform JSON string into a data.frame 
  rawDataFrameFromJSON <- fromJSON(rawDataFrameFromRestAPI)
  
  rawDataFrameFromList <- rawDataFrameFromJSON$DataFeed$Rows
  
  rawDataFrameFromList <- as.data.frame(rawDataFrameFromList)
  
}
