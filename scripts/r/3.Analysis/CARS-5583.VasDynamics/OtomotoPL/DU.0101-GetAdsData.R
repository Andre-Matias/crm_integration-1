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

vertical <- "otomotoPL"
file <- "ads"

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
print(filename)

# connect to database  ------------------------------------------------------
conDB <-  
dbConnect(
RMySQL::MySQL(),
username = cfOtomotoPLDbUser,
password = bi_team_pt_password,
host = "127.0.0.1",
port = 3317, 
dbname = cfOtomotoPLDbName
)

# get data ------------------------------------------------------------------
dbSqlQuery <-
paste(
"SELECT *, 'ads' AS tablename",
"FROM otomotopl.ads",
"WHERE created_at_first >= '", i, "00:00:00' AND created_at_first <= '", i,"23:59:59'",
"UNION ALL",
"SELECT *, 'ads_archive' AS tablename",
"FROM otomotopl.ads_archive",
"WHERE created_at_first >= '", i, "00:00:00' AND created_at_first <= '", i,"23:59:59'"
)

dfSqlQuery <-
dbGetQuery(conDB, dbSqlQuery)


# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)

# fix columns with switched values region_id e category_id ------------------

## create tmp columns ----
dfSqlQuery$region_id_tmp <- as.numeric(0)
dfSqlQuery$category_id_tmp <- as.numeric(0)

dfSqlQuery$category_id_tmp[dfSqlQuery$tablename =="ads_archive"] <-
dfSqlQuery$region_id[dfSqlQuery$tablename =="ads_archive"]

dfSqlQuery$region_id_tmp[dfSqlQuery$tablename =="ads_archive"] <-
dfSqlQuery$category_id[dfSqlQuery$tablename =="ads_archive"] 

dfSqlQuery$category_id[dfSqlQuery$tablename =="ads_archive"] <-
dfSqlQuery$category_id_tmp[dfSqlQuery$tablename =="ads_archive"]

dfSqlQuery$region_id[dfSqlQuery$tablename =="ads_archive"] <-
dfSqlQuery$region_id_tmp[dfSqlQuery$tablename =="ads_archive"]

## delete tmp colums
dfSqlQuery$region_id_tmp <- NULL
dfSqlQuery$category_id_tmp <- NULL

# write file to disk --------------------------------------------------------
s3saveRDS(
  x = dfSqlQuery, 
  bucket = bucket_path, 
  object = filename
)
}