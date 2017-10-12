# libraries -------------------------------------------------------------------
#library("config")
library("RMySQL")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("lubridate")
library("fasttime")
library("ggplot2")
library("ggthemes")
library("scales")
library("aws.s3")

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
         "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "stradia_ec")
)

# define query ----------------------------------------------------------------

cmdSqlQuery <-
  "
  SELECT id, posted
  FROM answers
  WHERE user_id = seller_id
  ;
  "

# connect to stradia db -------------------------------------------------------
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

# disconnect to db ------------------------------------------------------------

dbDisconnect(conDB)

dfStradiaAnswers <- as.data.frame(dfQueryResults)
rm(dfQueryResults)


# connect to stockars ---------------------------------------------------------

dbUsername <- "biuser"
dbPassword <- biUserPassword
dbHost <- "172.61.11.31"
dbPort <- "3306"
dbName <- "crm_cars_ec"

sshUser <- "biuser"
sshHost <- "52.11.38.25"
sshPort <- "10022"

dbLocalPort <- 10503
dbLocalHost <- "127.0.0.1"

system("killall ssh", wait=FALSE)

cmdSSH <-
  paste0(
    "ssh -i", " ",  sshKeyPath, " ", sshUser, "@", sshHost, " ", "-p", " ", 
    sshPort, " ", "-L", " ",  dbLocalPort, ":", dbHost ,":", dbPort," ", "-N"
  )

system(cmdSSH, wait=FALSE)

Sys.sleep(2)

conDB <-  dbConnect(RMySQL::MySQL(), username = dbUsername,
                    password = biUserPassword , host = dbLocalHost,
                    port = dbLocalPort , dbname = dbName)

sqlCmd <- 
  "
  SELECT m.external_id, m.sent_at, m.created_at, m.direction
  FROM messages m
  INNER JOIN messages_threads mt
  ON m.message_thread_id = mt.id
  INNER JOIN buyers b
  ON mt.buyer_id = b.id
  WHERE partner = 'stradia'
  ;
  "

dfSqlCmd <- dbGetQuery(conDB,sqlCmd)

dbDisconnect(conDB)

rawStockarsMessages <- as.data.frame(dfSqlCmd)

# -----------------------------------------------------------------------------



rawStockarsMessages$external_id <- 
  as.numeric(rawStockarsMessages$external_id)

dfStradiaAnswers$id <-
  as.numeric(dfStradiaAnswers$id)

df <-
  dfStradiaAnswers %>%
  filter(posted >= '2017-09-21 00:00:00') %>%
  left_join(rawStockarsMessages, by=c("id"="external_id")) %>%
  mutate(
    posted = fastPOSIXct(posted),
    sent_at = fastPOSIXct(sent_at),
    created_at = fastPOSIXct(created_at),
    synctime = 
      ifelse(direction == 'in',
             difftime(created_at, posted, "secs"),
             difftime(posted, created_at, "secs")
           ),
    brackets = cut(synctime, c(0, 5, 60, Inf), include.lowest = TRUE)
  )

dfStats <-
  df %>%
  mutate(dayhour = as.Date(format(posted, "%Y-%m-%d"))) %>%
  group_by(dayhour, brackets) %>%
  summarise(
    qtyByBracket = sum(!is.na(id))
  ) %>%
  mutate(perByBracket = qtyByBracket / sum(qtyByBracket)) %>%
  filter(dayhour >= Sys.Date() - 14)
  
# save it to amazon s3 --------------------------------------------------------

s3saveRDS(x = dfStats,
          object = "ecuador_stk_str_messages.RDS",
          bucket = "pyrates-data-ocean/GVPI-88")


dfStatsInOut <-
  df %>%
  mutate(day = as.POSIXct(format(posted, "%Y-%m-%d"))) %>%
  group_by(day, direction) %>%
  summarise(
    qtyByDirection = sum(!is.na(id))
  ) %>%
  filter(day >= Sys.Date() - 14)

s3saveRDS(x = dfStatsInOut,
          object = "ecuador_dfStatsInOut.RDS",
          bucket = "pyrates-data-ocean/GVPI-88")