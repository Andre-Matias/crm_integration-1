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
bucket_path <- "s3://pyrates-eu-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "autovitRO"
file <- "adpage"

startDate <- as.POSIXct("2017-10-01", tz = "UTC")
endDate <- as.POSIXct(as.character(Sys.Date()-1), tz = "UTC")

dates <-
  seq(startDate, endDate, "day")

events <-
  as.character(
    paste(
      c(
        '\\\'ad_page\\\''
      ),
      collapse = ","
    )
  )

listServerPath <-
  list(
    #otomoto = c('/h/v-otomoto-android', '/h/v-otomoto-ios', '/h/v-otomoto-web')
    autovit = c('/h/v-autovit-android', '/h/v-autovit-ios', '/h/v-autovit-web', 'verticals_hydra_autovit_history_hydra')
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
      max = Inf, prefix = paste(sep = "/", "datalake", vertical, file)
    )
  )

# check existing files --------------------------------------------------------

listExistingDates <-
  as.POSIXct(
    unique(
      str_extract(s3ExistingsObjects$Key, 
                  paste0("[0-9]{8}")
      )
    ), format="%Y%m%d", tz = "UTC"
  )

# dates to get ----------------------------------------------------------------

listDatesToGet <-
  as.POSIXct(as.character(dates[!(dates %in% listExistingDates)]), tz = "UTC")

for(date in listDatesToGet){
  
  epochDate <- as.integer(date)
  yearText <- as.character(format(as.POSIXct(date, origin = '1970-01-01'), "%Y"))
  monthText <- as.character(format(as.POSIXct(date, origin = '1970-01-01'), "%m"))
  dayText <- as.character(format(as.POSIXct(date, origin = '1970-01-01'), "%d"))
  
  for(server in head(listServerPath, 1)){
    
    server_path <- server[1]
    schema <- server[4]
    
    awsObject <- 
      paste0(
        paste(sep = "/", "datalake", vertical, file), "/",file, "_",
        gsub("/h/", "", server[4]), "_", paste0(yearText, monthText,dayText)
      )
    
    sqlQuery <-
    "
    SELECT
    partition_0, year, day, month, params_en, params_ad_id, COUNT(*)qtyEvents
    FROM spectrum_redshift.{schema}
    WHERE params_cat_l1_id = 29
    AND params_en IN({events})
    AND year = \\'{yearText}\\'
    AND month = \\'{monthText}\\'
    AND day = \\'{dayText}\\'
    GROUP BY partition_0, year, day, month, params_en, params_ad_id
    ;
    "
    
    sqlQuery <- glue(sqlQuery)
    
    sqlUnload <-
      "  
    UNLOAD (\'{sqlQuery}\')
    to '{bucket_path}{awsObject}_'
    CREDENTIALS 'aws_access_key_id={myS3key};aws_secret_access_key={MyS3SecretAccessKey}'
    DELIMITER AS '\\t'
    "
    
    sqlUnload <- glue(sqlUnload)
    
    print(sqlUnload)
    
  }    
    
    # connect to Yamato ----------------------------------------------------
    
    drv <- dbDriver("PostgreSQL")
    
    conDB <-
      dbConnect(
        drv,
        host = "10.101.5.237", # dwYamatoDbHost,
        port = dwYamatoDbPort,
        dbname = dwYamatoDbName,
        user = dwYamatoDbUsername,
        password = dwYamatoDbPassword
      )
    
    print(paste(monthText, server_path, epochDate))
    print(sqlQuery)
    
    # dbSqlQuery <-
    #   RPostgreSQL::dbGetQuery(
    #     conDB, sqlUnload
    #   /
    
    dbDisconnect(conDB)
  }
}

