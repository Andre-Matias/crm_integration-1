# config ----------------------------------------------------------------------
options(scipen=999)

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

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
library("fasttime")
library("forcats")
library("RColorBrewer")
library("gridExtra")
library("grid")
library("aws.s3")

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
    AND country_id IN (218)
    AND MESSAGE_TEXT != 'este producto ya no se encuentra disponible.'
    AND date > '2017-10-01 00:00:00'
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
dbName <- "crm_cars_ec"

sshUser <- "biuser"
sshHost <- "52.11.38.25"
sshPort <- "10022"

dbLocalPort <- 10004
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
  "
  SELECT m.external_id, m.sent_at, m.created_at, m.direction
  FROM messages m
  INNER JOIN messages_threads mt
  ON m.message_thread_id = mt.id
  INNER JOIN buyers b
  ON mt.buyer_id = b.id
  WHERE partner = 'olx'
  AND m.created_at >= '2017-10-01 00:00:00'
  ;
  "
  
  dfSqlCmd <- dbGetQuery(conDB,sqlCmd)
  rawStockarsMessages <- as.data.frame(dfSqlCmd)

sqlCmd <- 
"
SELECT external_id, A.id_product, email
FROM (
  SELECT
  id_product,
  external_id
  FROM export_ad
  WHERE
  external_id IS NOT NULL AND external_id != '' AND partner_name = 'olx'
)A
INNER JOIN
(SELECT id_product, modify_user FROM stock_product)B
ON A.id_product = B.id_product
INNER JOIN
(SELECT id_user, email, status FROM user)C
ON B.modify_user = C.id_user
;
"
dfSqlCmd <- dbGetQuery(conDB,sqlCmd)
rawStockarsExport<- as.data.frame(dfSqlCmd)

dbDisconnect(conDB)

df <- 
  rawStockarsPoseidonMessages %>%
  mutate(message_id = as.character(message_id),
         item_id = as.character(item_id)) %>%
  left_join(rawStockarsMessages, by = c("message_id"="external_id"))%>%
  left_join(rawStockarsExport, by = c("item_id"="external_id"))%>%
  mutate(dateorigin2 = fastPOSIXct(dateorigin, tz = "UTC"),
         message_date2 = fastPOSIXct(sent_at, tz = "UTC"),
         create_date2 = fastPOSIXct(created_at, tz = "UTC"),
         diffSyncTime = 
           as.numeric(
             difftime(create_date2, message_date2, tz = "UTC", units = "mins")
           ),
         diffSyncIntervals = 
           cut(diffSyncTime, 
               breaks = c(0, 0.08333333, 1, 10, 60, 240, Inf),
               dig.lab=10)
  ) %>%
  filter(dateorigin2 >= '2017-10-01 00:00:00') %>%
  arrange(dateorigin2)

dfStats <- 
  df %>% 
  mutate(dayorigin = as.Date(dateorigin)) %>%
  group_by(dayorigin) %>%
  summarise(qtyMessagesPoseidon = sum(!is.na(message_id)), 
            qtyMessagesStockars = sum(!is.na(message_date2))) %>%
  mutate(var = qtyMessagesStockars/qtyMessagesPoseidon-1) %>%
  filter(dayorigin > as.Date(Sys.time())-17)

dfStatsSyncTime <- 
  df %>% 
  mutate(dayorigin = as.Date(dateorigin),
         diffSyncIntervals = fct_recode(diffSyncIntervals,
                                        "< 5 secs"          = "(0,0.08333333]",
                                        "< 1 min"           = "(0.08333333,1]",
                                        "1 min - 10 mins"   = "(1,10]",
                                        "10 mins - 1 hour"  = "(10,60]",
                                        "1 hour - 4 hours"  = "(60,240]",
                                        "> 4 hours"         = "(240,Inf]"
         )
  ) %>%
  group_by(dayorigin, diffSyncIntervals) %>%
  summarise(qtyByCut = sum(!is.na(diffSyncIntervals))) %>%
  mutate(perByCut = qtyByCut / sum(qtyByCut)) %>%
  filter(dayorigin > as.Date(Sys.time())-17,
         !is.na(diffSyncIntervals)
  )

ghQuantityMessagesSynced <-
  ggplot(dfStats)+
  geom_bar(stat = "identity", 
           aes(dayorigin, qtyMessagesPoseidon), fill="#BEC100")+
  geom_bar(stat = "identity", 
           aes(dayorigin, qtyMessagesStockars), fill="royalblue3")+
  geom_text(
    aes(dayorigin, qtyMessagesStockars, label=percent(round(var, 2))),
    vjust=-0.5,family = "Andale Mono")+
  scale_x_date(
    date_breaks = "1 day", date_labels = "%d\n%b\n%y")+
  theme_fivethirtyeight()+theme(text=element_text(family = "Andale Mono"))+
  ggtitle("OLX/Stockars.EC - Quantity of Messages synced") + 
  geom_text(aes(x=dayorigin, y=0, label=qtyMessagesStockars), vjust = -0.1, 
            family = "Andale Mono", colour="white")

ghSyncingTime <- 
  ggplot(dfStatsSyncTime) + 
  geom_bar(stat="identity", aes(x=dayorigin, y=perByCut, fill=diffSyncIntervals)
           )+
  scale_x_date(
    date_breaks = "1 day", date_labels = "%d\n%b\n%y")+
  scale_y_continuous(labels = percent)+
  scale_fill_brewer(palette = "RdYlGn", type="seq", direction = -1, drop=FALSE,
                    guide = guide_legend(title = "Sync Intervals"))+
  theme_fivethirtyeight()+theme(text=element_text(family = "Andale Mono"))+
  theme(legend.position="bottom")+
  ggtitle("OLX/Stockars.EC - Syncing Time") + 
  geom_text(data = dfStats, aes(x=dayorigin, y=0, label=qtyMessagesStockars),
            vjust = -0.1, family = "Andale Mono")


# list of users that messages are not syncing ---------------------------------
dfUsersNotSincying <-
  df %>%
  filter(!is.na(message_id), 
         is.na(message_date2), 
         dateorigin >= as.Date(Sys.time())-17,
         !is.na(email)
         ) %>%
  group_by(email) %>%
  summarise(qtyMessagesPerEmail = sum(!is.na(message_id))) %>%
  arrange(-qtyMessagesPerEmail)

write.table(x = dfUsersNotSincying,
            file = "~/tmp/dfUsersNotSincying.txt",
            col.names = TRUE,
            row.names = FALSE)

# # messages sent from stockars -------------------------------------------------
# dfSentMessagesFromStockars <-
#   rawStockarsMessages[ ,c("message_date", "direction")] %>%
#   filter(direction == 0) %>%
#   mutate(MessageDay = as.Date(message_date)) %>%
#   group_by(MessageDay) %>%
#   summarise(qtyMessagesSent = sum(!is.na(MessageDay)))

# GRAPH: messages sent from stockars ------------------------------------------

# align axis and build final graph --------------------------------------------

gb1 <- ggplot_build(ghQuantityMessagesSynced)
gb2 <- ggplot_build(ghSyncingTime)

n1 <- length(gb1$layout$panel_params[[1]]$y.labels)
n2 <- length(gb2$layout$panel_params[[1]]$y.labels)


gA <- ggplot_gtable(gb1)
gB <- ggplot_gtable(gb2)


g <- rbind(gA, gB, size = "last")

EC_g <- g

save(list = c("EC_g"), file = "EC_g.Rdata")

put_object(file = "EC_g.Rdata", object = "EC_g.Rdata",
           bucket = "pyrates-data-ocean/GVPI-85")

  