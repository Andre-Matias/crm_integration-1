# load libraries --------------------------------------------------------------
library("aws.s3")
library("dplyr")
library("data.table")
library("dtplyr")
library("stringr")
library("RMySQL")
library("slackr")
library("glue")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")
slackrSetup()

# config ----------------------------------------------------------------------
bucket_path <- "s3://pyrates-data-ocean/"

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

vertical <- "autovitRO"
file <- "MessagesOnAtlas"

startDate <- "2017-10-01"
endDate <- as.character(Sys.Date()-1)

AllDates <- seq.Date(as.Date(startDate), as.Date(endDate), 1)

# getting file list -----------------------------------------------------------
s3ExistingsObjects <- 
  as_tibble(
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
  print(filename)

  # specify query ---------------------------------------------------------
  dbSqlQuery <-
      "SELECT ad_id, DATE(posted)posted_date, COUNT(*)qtyMessagesOnAtlas FROM answers
      WHERE parent_id = 0
      AND user_id = seller_id
      AND buyer_id = sender_id
      AND buyer_id != seller_id
      AND posted >= '{i} 00:00:00' AND posted <= '{i} 23:59:59'
      GROUP BY ad_id, DATE(posted)
      ;
      "
  
dbSqlQuery <- glue(dbSqlQuery)

print(dbSqlQuery)

conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = "bi_team_pt",
    password = bi_team_pt_password,
    host = "127.0.0.1",
    port = 3315, 
    dbname = "autovitro"
  )

  dfSqlQuery <-
    dbGetQuery(conDB, dbSqlQuery)
  
  # disconnect from database  -------------------------------------------------
  dbDisconnect(conDB)
  
  # write file to disk --------------------------------------------------------
  s3saveRDS(
    x = dfSqlQuery, 
    bucket = bucket_path, 
    object = filename
  )
}