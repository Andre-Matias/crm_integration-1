# load libraries --------------------------------------------------------------
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("RPostgreSQL")
library("glue")
library("aws.s3")
library("slackr")
library("stringr")


# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "otomotoPL"
file <- "adsimpressions"

startDate <- as.POSIXct("2017-10-01")
endDate <- as.POSIXct(as.character(Sys.Date()-1))

dates <-
  seq(startDate, endDate, "day")

events <-
  as.character(
    paste(
      c(
        'ads_impression',
        'ads_view'
      ),
      collapse = "','"
    )
  )

listServerPath <-
  list(
    otomoto = c('/h/v-otomoto-android', '/h/v-otomoto-ios', '/h/v-otomoto-web')
    #autovit = c('/h/v-autovit-android', '/h/v-autovit-ios', '/h/v-autovit-web'),
    #standvirtual = c('/h/v-standvirtual-android', '/h/v-standvirtual-ios', '/h/v-standvirtual-web'),
    #otodom = c('/h/v-otodom-android', '/h/v-otodom-ios', '/h/v-otodom-web'),
    #imovirtual = c('/h/v-imovirtual-android', '/h/v-imovirtual-ios', '/h/v-imovirtual-web'),
    #storia = c('/h/v-storia-android', '/h/v-storia-ios', "/h/v-storia-web")
  )

# getting file list -----------------------------------------------------------
s3ExistingsObjects <- 
  as.data.frame(
    get_bucket(
      bucket = bucket_path,
      max = Inf, prefix = paste(sep = "/", "datalake", vertical, file, "RDS")
    )
  )

# check existing files --------------------------------------------------------

listExistingDates <-
  as.POSIXct(
    unique(
      str_extract(s3ExistingsObjects$Key, 
                paste0("[0-9]{4}-[0-9]{2}-[0-9]{2}")
                )
    )
    )

# dates to get ----------------------------------------------------------------

listDatesToGet <-
  as.POSIXct(as.character(dates[!(dates %in% listExistingDates)]))

for(date in listDatesToGet){
  
  epochDate <- as.integer(date)
  monthText <- as.character(format(as.POSIXct(date, origin = '1970-01-01'), "%Y%m"))
  dayText <- as.character(format(as.POSIXct(date, origin = '1970-01-01'), "%Y%m%d"))
  
  
  for(server in listServerPath){
    
    for(i in 1:3){
      
      server_path <- as.character(server[i])
      print(server[i])
      
      if (grepl("android", server[i])){
        sqlQuery <- 
          glue(
            "
            SELECT server_path, server_date_trunc, eventname, item_id, COUNT(*)qty
            FROM hydra.verticals_ninja_android_{monthText}
            WHERE server_path = '{server_path}'
            AND server_date_trunc = {epochDate}
            AND eventname IN ('{events}')
            GROUP BY 1, 2, 3, 4
            "
            )
      } else if (grepl("ios", server[i])){
        sqlQuery <- 
          glue(
            "
            SELECT server_path, server_date_trunc, eventname, item_id, COUNT(*)qty
            FROM hydra.verticals_ninja_ios_{monthText}
            WHERE server_path = '{server_path}'
            AND server_date_trunc = {epochDate}
            AND eventname IN ('{events}')
            GROUP BY 1, 2, 3, 4
            "
            )
      } else if (grepl("web", server[i])){
        sqlQuery <-
          glue(
            "
            SELECT server_path, server_date_trunc, eventname, item_id, COUNT(*)qty
            FROM hydra.verticals_ninja_web_{monthText}
            WHERE server_path = '{server_path}'
            AND server_date_trunc = {epochDate}
            AND eventname IN ('{events}')
            GROUP BY 1, 2, 3, 4
            "
            )
      }
      
      # connect to Triton Silver ----------------------------------------------------
      
      drv <- dbDriver("PostgreSQL")
      
      conDB <-
        dbConnect(
          drv,
          host = dwTritonSilverDbHost,
          port = dwTritonSilverDbPort,
          dbname = "olxgroupbi",
          user = myTritonUser,
          password = myTritonPass
        )
      
      print(paste(monthText, server_path, epochDate))
      print(sqlQuery)
      
      print(paste(Sys.time(), "=> Starting"))
      
      dbSqlQuery <-
        dbSendQuery(
          conDB, sqlQuery
        )
      
      # dfRequestDB <- dbFetch(requestDB)
      
      dfSqlQuery <- data.frame()
      chunk <- data.frame()
      
      while (!dbHasCompleted(dbSqlQuery)) {
        
        chunk <- dbFetch(dbSqlQuery, n = 10000)
        
        print(
          paste(
            Sys.time(),
            nrow(chunk),
            sep = " | "
          )
        )
        
        text_slackr(channel = c("gv-bi-reporting"), 
                    text = paste(
                      Sys.time(),
                      nrow(chunk),
                      sep = " | "
                    )
        )
        
        if(nrow(dfSqlQuery) == 0){
          dfSqlQuery <- chunk
        } else {
          dfSqlQuery <- rbind(dfSqlQuery, chunk)
        }
        
      }
      
      dbClearResult(dbListResults(conDB)[[1]])
      
      print(paste0(gsub("/h/", "", server[i]), "_", dayText, ".RDS"))
      
      print(paste0(paste(sep = "/", "datalake", vertical, file), "/",
                   gsub("/h/", "", server[i]), "_", as.Date(dayText, "%Y%m%d"), ".RDS"))
      
      s3saveRDS(
        x = dfSqlQuery,
        bucket = bucket_path,
        object = paste0(paste(sep = "/", "datalake", vertical, file), "/",
                        gsub("/h/", "", server[i]), "_", as.Date(dayText, "%Y%m%d"), ".RDS")
      )
      dbDisconnect(conDB)
      }
    }
  }

