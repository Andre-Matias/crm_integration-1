# load libraries --------------------------------------------------------------
library("RPostgreSQL")
library("aws.s3")
library("purrr")
library("jsonlite")

# config ----------------------------------------------------------------------
options(scipen = 999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

bucket_path <- "s3://pyrates-data-ocean/"

hosts <- c("/h/v-otomoto-web")

# define range
startDate <- "2017-10-01"
endDate <- as.character(Sys.Date()-1)
endDate <- "2017-10-02"

rangeDate <- as.character(seq(as.Date(startDate),as.Date(endDate),by = 1))

# getting file list -----------------------------------------------------------
s3ExistingsObjects <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = "datalake/otomotoPL/searches/v-otomoto-web/"
    )
  )

# check existing files --------------------------------------------------------
listExistingDates <-
  as.Date(str_extract(s3ExistingsObjects$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------
listDatesToGet <-
  as.character(rangeDate[!(rangeDate %in% as.character(listExistingDates))])

# connect to silver -----------------------------------------------------------
drv <- dbDriver("PostgreSQL")

for(host in hosts){
  for(i in listDatesToGet){
    
    print (paste(host, "=>", i, "=> started at:", Sys.time()))
    
    conDB <- 
      dbConnect(
        drv, 
        host= dwTritonSilverDbHost,
        port = dwTritonSilverDbPort,
        dbname = dwTritonSilverDbName,
        user = myTritonUser,
        password = myTritonPass
      )
    
    sqlQuery <-
      "
    SELECT *
    FROM hydra.verticals_ninja_web_##partition##
    WHERE trackpage = 'listing'
    AND server_path = '##host##'
    AND user_id IS NOT NULL AND server_date_trunc = date_part(epoch, '##Date##')
    ;
    "
    
    sqlQuery <- as.character(gsub("##Date##", i,  sqlQuery))
    sqlQuery <- as.character(gsub("##host##", host,  sqlQuery))
    
    sqlQuery <- as.character(gsub("##partition##", gsub("-", "", substr(i, 1, 7)), sqlQuery))
    
    requestDB <- 
      dbSendQuery(
        conDB,
        sqlQuery
      )
    
    dfRequestDB <- dbFetch(requestDB)
    dbClearResult(dbListResults(conDB)[[1]])
    dbDisconnect(conDB)
    
    dfHydraResults <- dfRequestDB
    
    rm("dfRequestDB")
    
    dfHydraResults$extra <- gsub("\"\\[", "\\[", dfHydraResults$extra)
    dfHydraResults$extra <- gsub("\\]\"", "\\]", dfHydraResults$extra)
    dfHydraResults$extra <- lapply(dfHydraResults$extra, function(x) paste0(x, collapse=""))
    dfHydraResults$a <- purrr::map(dfHydraResults$extra, jsonlite::validate)
    dfHydraResults <- dfHydraResults[dfHydraResults$a == TRUE, ]
    
    print (paste(host, "=>", i, "=> started at:", Sys.time()), "Saving to AWS")
    
    s3saveRDS(x = dfHydraResults,
              object = paste0("Results_", gsub("/h/", "", host), "_", i, ".RDS"),
              bucket = paste0("pyrates-data-ocean/datalake/otomotoPL/searches/", gsub("/h/", "", host))
    )

    print (paste(host, "=>", i, "=> started at:", Sys.time()), "Saved to AWS")
  }
}

