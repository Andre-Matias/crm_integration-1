# config ----------------------------------------------------------------------
options(scipen=999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

# load libraries --------------------------------------------------------------
library("RMySQL")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

# -----------------------------------------------------------------------------

vertical_cf <- "StradiaIn"


dbUser <- get(paste0("cf", vertical_cf, "DbUser")) 
dbPass <- bi_team_pt_password
dbHost <- as.character(
  ifelse(Sys.info()["nodename"] == "bisb", "127.0.0.1"
         , get(paste0("cf", vertical_cf, "DbHost")))
)
dbPort <- as.numeric(get(paste0("cf", vertical_cf, "DbPort")))
dbName <- get(paste0("cf", vertical_cf, "DbName")) 

# connect to database  ------------------------------------------------------
conDB <-  
  dbConnect(
    RMySQL::MySQL(),
    username = dbUser,
    password = dbPass,
    host = dbHost,
    port = dbPort, 
    dbname = dbName
  )

dbSqlQuery <-
  paste(
  "SELECT A.id, A.status, U.email", 
  "FROM ads A",
  "INNER JOIN",
  "users U",
  "ON A.user_id = U.id",
  "WHERE A.`status` = 'active'",
  ";"
  )

dfSqlQuery <-
  dbGetQuery(conDB,dbSqlQuery)

dfStradiaActiveAds <- dfSqlQuery

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)

# connect to stockars ---------------------------------------------------------

dbUsername <- "biuser"
dbPassword <- biUserPassword
dbHost <- "scindbbi.row"
dbPort <- "3306"
dbName <- "crm_cars_in"

sshUser <- "biuser"
sshHost <- "34.251.141.34"
sshPort <- "10022"

dbLocalPort <- 10103
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
                    password = dbPassword , host = dbLocalHost,
                    port = dbLocalPort , dbname = dbName)

dbSqlQuery <- 
  paste(
    "SELECT", 
    "U.id_user, U.email, U.`status` as user_status,",
    "D.status as division_status, D.id_division",
    "FROM user U",
    "INNER JOIN user_division UD",
    "ON U.id_user = UD.id_user",
    "INNER JOIN division D",
    "ON UD.id_division = D.id_division"
    )

dfSqlQuery <-
  dbGetQuery(conDB,dbSqlQuery)

rawStockarsUsersEmails <- dfSqlQuery

dbDisconnect(conDB)

# clear results ---------------------------------------------------------------
rm("dfSqlQuery")

df <-
  dfStradiaActiveAds %>%
  left_join(rawStockarsUsersEmails, by=c("email"="email"))

write.table(x = df,
            file = paste0("~/tmp/","StockarsIndiaActiveAdsInactiveUsers.txt"),
            sep = "\t",row.names = FALSE, col.names = FALSE)

dfStats <-
  df %>%
  group_by(status, user_status, division_status) %>%
  summarise(
    qty = sum(n())
  )


# users status dictionary -----------------------------------------------------
# Status (mapping)
# Inactive = 0;
# Active = 1;
# Password_Expired = 2;
# Suspended = 3;
# Invited = 4;
# Invited_Ready_To_Import = 5;