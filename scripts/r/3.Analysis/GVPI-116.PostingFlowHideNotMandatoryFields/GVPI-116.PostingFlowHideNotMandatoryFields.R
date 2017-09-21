# libraries -------------------------------------------------------------------
library("config")
library("RMySQL")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("lubridate")

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# define empty data.frame -----------------------------------------------------

dfAll <- data.frame()

for(site in c("standvirtual_pt", "otomoto_pl", "autovit_ro")){
  
dfTemp <-data.frame()
print(site)
# get database config
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", site)
)

# define query ----------------------------------------------------------------

cmdSqlQuery <-
  "
  SELECT A.id, A.created_at_first, A.description, B.name_en
    FROM ads A
  INNER JOIN categories B
  ON A.category_id = B.id
  WHERE 
    created_at_first >= '2017-05-01 00:00:00'
    AND created_at_first < CURRENT_DATE()
    AND net_ad_counted = 1
    AND B.parent_id = 0
UNION ALL
  SELECT A.id, A.created_at_first, A.description, C.name_en
    FROM ads A
  INNER JOIN categories B
  ON A.category_id = B.id
  INNER JOIN categories C
  ON B.parent_id = C.id
  WHERE 
    created_at_first >= '2017-05-01 00:00:00'
    AND created_at_first < CURRENT_DATE()
    AND net_ad_counted = 1
    AND B.parent_id != 0
  ;
  "

# connect to db ---------------------------------------------------------------
conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= bi_team_pt_password,
                  host= ifelse(Sys.info()["nodename"] =="bisb",
                               "127.0.0.1",
                               config$DbHost),
                  port = ifelse(Sys.info()["nodename"] =="bisb",
                               config$BiServerPort,
                               config$DbPort),
                  dbname = config$DbName
)


# get data --------------------------------------------------------------------
dfQueryResults <- 
  dbGetQuery(conDB,cmdSqlQuery)

dbDisconnect(conDB)

# summarise data --------------------------------------------------------------

dfTemp <-
  dfQueryResults %>%
  mutate(
    platform = site, 
    day = as.Date(created_at_first),
    week = floor_date(day, "week"),
    DescriptionState = ifelse(!is.na(description) 
                              & nchar(description, type = "bytes") > 1,
                              "with description",
                              "without description")
  ) %>%
  group_by(platform, day, week, name_en, DescriptionState) %>%
  summarise(qtyListings = sum(n()))

if(nrow(dfAll)==0){
    dfAll <- dfTemp}
  else{
    dfAll <- rbind(dfAll, dfTemp)
  }
}


# save it to amazon s3 --------------------------------------------------------

s3saveRDS(x = dfAll,
          object = "DescriptionState.RDS",
          bucket = "pyrates-data-ocean/GVPI-116")