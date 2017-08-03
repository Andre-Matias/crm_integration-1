# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")

# load libraries --------------------------------------------------------------

library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("RMySQL")
library("RPostgreSQL")
library("tidyr")
library("scales")
library("ggplot2")
library("ggthemes")

# connect to poseidon ---------------------------------------------------------
drv <- dbDriver("PostgreSQL")

conDB <- 
  dbConnect(
    drv, 
    host="bi-analytics.cnsuxis6zqxr.us-west-2.redshift.amazonaws.com",
    port = "5439",
    dbname = "analytics",
    user = userPoseidon,
    password = passPoseidon
    )

requestDB <- 
  dbSendQuery(
    conDB,
        "SELECT A.message_id, B.conversation_id, A.sender_id,
        A.country_id, A.platform_id, CONVERT(varchar, A.date) as DateOrigin,
        A.message_text, B.item_id
        FROM
        (
        SELECT *
        FROM ods_naspers.ft_h_messages
        WHERE conversation_id IN
        (
        SELECT conversation_id
        FROM ods_naspers.ft_h_conversations
        WHERE item_id IN
        (
        SELECT item_id
        FROM ods_naspers.ft_h_listing
        WHERE country_id IN (32, 170, 218, 604)
        AND category_l2_id = 378
        AND device_source_id = 27
        AND platform_id = 1 AND live_id = 1
        AND time_id >= '2017-01-01 00:00:00'
        )
        )
        AND country_id IN (32) -- 32 Argentina
        AND MESSAGE_TEXT != 'este producto ya no se encuentra disponible.'
        
        )A
        INNER JOIN 
        (SELECT item_id, conversation_id FROM ods_naspers.ft_h_conversations) B
        ON A.conversation_id=B.conversation_id
        ;"
      )

dfRequestDB <- dbFetch(requestDB)
dbClearResult(dbListResults(conDB)[[1]])
dbDisconnect(conDB)

rawStockarsPoseidonMessages <- dfRequestDB

rm("dfRequestDB")

# connect to stockars ---------------------------------------------------------

dbUsername <- "biuser"
dbPassword <- biUserPassword
dbHost <- "172.61.11.31"
dbPort <- "3306"
dbName <- "crm_cars_ar"

sshUser <- "biuser"
sshHost <- "52.33.194.191"
sshPort <- "10022"

dbLocalPort <- 10003
dbLocalHost <- "127.0.0.1"

system("killall ssh", wait=FALSE)

cmdSSH <-
  paste0(
    "ssh -i", " ",  sshKeyPath, " ", sshUser, "@", sshHost, " ", "-p", " ", 
    sshPort, " ", "-L", " ",  dbLocalPort, ":", dbHost ,":", dbPort," ", "-N"
  )

system(cmdSSH, wait=FALSE)

Sys.sleep(5)
conDB <-  dbConnect(RMySQL::MySQL(), username = dbUsername,
                    password = dbPassword , host = dbLocalHost,
                    port = dbLocalPort , dbname = dbName)

sqlCmd <- 
  "SELECT * FROM message A 
  LEFT JOIN
    (SELECT id_thread, partner_name FROM message_thread) B
  ON A.id_thread = B.id_thread
  WHERE message_date >= '2017-01-01';
  "

dfSqlCmd <- dbGetQuery(conDB,sqlCmd)

dbDisconnect(conDB)

# -----------------------------------------------------------------------------
rawStockarsMessages <- as.data.frame(dfSqlCmd)

df <- 
  rawStockarsPoseidonMessages %>%
  mutate(message_id = as.character(message_id), 
         dateorigin = as.POSIXct(strptime(dateorigin, "%Y-%m-%d %H:%M:%S"))) %>%
  left_join(rawStockarsMessages, by=c("message_id"="external_message_id")) %>%
  filter(dateorigin >= '2017-01-20 00:00:00') %>%
  arrange(dateorigin)

dfStats <- 
  df %>% 
  mutate(dayorigin=as.Date(dateorigin)) %>% 
  group_by(dayorigin) %>% 
  summarise(qtyMessagesPoseidon=sum(!is.na(message_id)), 
            qtyMessagesStockars=sum(!is.na(id_message))) %>%
  mutate(var = qtyMessagesStockars/qtyMessagesPoseidon-1) %>%
  filter(dayorigin > as.Date(Sys.time())-17)
--
ggplot(dfStats)+
  geom_bar(stat = "identity", 
           aes(dayorigin, qtyMessagesPoseidon), fill="#BEC100")+
  geom_bar(stat = "identity", 
           aes(dayorigin, qtyMessagesStockars), fill="royalblue3")+
  geom_text(
    aes(dayorigin, qtyMessagesStockars, label=percent(round(var, 2))),
    vjust=-0.5,family = "Andale Mono")+
  scale_x_date(date_breaks = "1 day", date_labels = "%d\n%b\n%y")+
  theme_fivethirtyeight()+theme(text=element_text(family = "Andale Mono"))+
  ggtitle("OLX/Stockars.AR - Quantity of Messages synced")

# -----------------------------------------------------------------------------