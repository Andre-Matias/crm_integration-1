# load libraries --------------------------------------------------------------
library("aws.s3")
library("data.table")
library("stringr")
library("RMySQL")
library("slackr")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "autovitRO"
file <- "ads_history"

startDate <- "2017-01-01"
endDate <- as.character(Sys.Date()-1)

AllDates <- seq.Date(as.Date(startDate), as.Date(endDate), 1)

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
  as.Date(str_extract(s3ExistingsObjects$Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}"))

# dates to get ----------------------------------------------------------------

listDatesToGet <-
  as.character(AllDates[!(AllDates %in% listExistingDates)])

# get files ----------

for (i in listDatesToGet) {
filename <- paste0("/datalake/", vertical,"/", file, "/", "RDS/",
                   "RDL_",vertical, "_", file ,"_",  i, ".RDS")
print(
  paste(
    Sys.time(),
    filename,
    sep = " | "
  )
)


text_slackr(channel = c("gv-bi-reporting"), 
            text = paste(
              Sys.time(),
              filename,
              sep = " | "
            )
              )

# connect to database  ------------------------------------------------------
conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = "bi_team_pt",
    password = bi_team_pt_password,
    host = "127.0.0.1",
    port = 3315, 
    dbname = "autovitro"
  )
# get data ------------------------------------------------------------------
dbSqlQuery <-
  paste(
    "SELECT `primary`, changed_at, id, `status`, params",
    "FROM ads_history.ads_history_autovitro", 
    "WHERE changed_at", 
    "BETWEEN '", i, " 00:00:00'", 
    "AND '", i, " 23:59:59';"
  )

dbSqlQuery <-
  dbSendQuery(conDB,dbSqlQuery)

dfSqlQuery <- data.frame()

chunk <- data.frame()

while (!dbHasCompleted(dbSqlQuery)) {
  
  chunk <- dbFetch(dbSqlQuery, n = 100000)
  
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
  
  
  if(nrow(dfSqlQuery)==0){
    dfSqlQuery <- chunk
  } else {
    dfSqlQuery <- rbind(dfSqlQuery, chunk)
  }
  
}
# free the result set
dbClearResult(dbSqlQuery)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)

# write file to disk --------------------------------------------------------
text_slackr(channel = c("gv-bi-reporting"), 
            text = 
              paste(
                Sys.time(),
                "Saving to AWS",
                sep = " | "
              )
)

print(paste(
  Sys.time(),
  "Saving to AWS",
  sep = " | "
)
)

s3saveRDS(
  x = dfSqlQuery, 
  bucket = bucket_path, 
  object = filename
)

text_slackr(channel = c("gv-bi-reporting"), 
            text = 
              paste(
                Sys.time(),
                "Saved to AWS",
                sep = " | "
                )
)

print(
paste(
  Sys.time(),
  "Saved to AWS",
  sep = " | "
)
)
}
