# libraries -------------------------------------------------------------------
library("config")
library("RMySQL")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("stringr")
library("ggthemes")
library("scales")
library("lubridate")
library("tm")

# config ----------------------------------------------------------------------
options(scipen = 9999)

#for(site in c("standvirtual_pt", "otomoto_pl", "autovit_ro")){

for(site in c("otomoto_pl")){
  
  # load db configurations ------------------------------------------------------
  config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                        config = Sys.getenv("R_CONFIG_ACTIVE", site)
  )
  
  # -----------------------------------------------------------------------------
  load("~/credentials.Rdata")
  
  # get data --------------------------------------------------------------------
  conDB<- 
    dbConnect(MySQL(), 
              user= config$DbUser, 
              password= bi_team_pt_password,  
              host = "127.0.0.1",
              port = as.numeric(config$BiServerPort),
              dbname = config$DbName
    )
  
  dbGetQuery(conDB, "SET NAMES utf8")

  cmdSqlQuery <-
  "
    SELECT * FROM answers A
    LEFT JOIN (SELECT id AS l_id, user_id as l_user_id, category_id as l_category_id FROM ads) L
      ON A.ad_id = L.l_id
    LEFT JOIN (SELECT id as u_id, is_business AS u_is_business FROM users) U
      ON L.l_user_id = U.u_id
    LEFT JOIN (SELECT id as c_id, name_en FROM categories) C
      ON L.l_category_id = C.c_id
    WHERE posted >= '2017-09-01 00:00:00'
    AND posted < '2018-01-01 00:00:00'
  "
  
  dbSendQuery(conDB, 'set character set "utf8"')
  
  dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)
  dfMessages <- as_tibble(dfQueryResults)
  dbDisconnect(conDB)

  rm("dfQueryResults")

}

# save data to local storage TODO: save it to S3 ------------------------------
#saveRDS(dfMessages, "~/dfMessages.RDS")

# remove spam and sort messages -----------------------------------------------
dfMessages <- 
  dfMessages %>%
  filter(spam_status != "spam", !is.na(u_is_business)) %>%
  arrange(buyer_id, seller_id, ad_id, posted)

# keep only the first message sent by the buyer -------------------------------
dfMessages_L0 <-
  dfMessages %>%
  filter(parent_id == 0,
         seller_id != buyer_id,
         sender_id == buyer_id,
         user_id == buyer_id)

# filter message for Sep, Oct, Nov 2017 and count number of messages in thread
dfStats <- 
  dfMessages_L0 %>%
  filter(posted >= '2017-09-01-01 00:00:00' 
         & posted < '2017-12-01-01 00:00:00')%>%
  group_by(topics_count) %>%
  summarise(qtyTopicsCount = sum(n())) %>%
  mutate(perTopicsCount = qtyTopicsCount / sum(qtyTopicsCount))

# plot it ---------------------------------------------------------------------
ggplot(dfStats)+
  geom_bar(stat="identity", aes(x = topics_count, y = perTopicsCount))+
  scale_x_continuous(limits = c(0, 8), breaks = seq(1,8,1))+
  scale_y_continuous(limits = c(0, 1), labels = percent)+
  geom_label(
    aes(x = topics_count, 
        y = perTopicsCount, 
        label = percent(round(perTopicsCount, 3))), vjust = -1
    )+
  geom_text(
    aes(x = topics_count, 
        y = perTopicsCount, 
        label = qtyTopicsCount, vjust = -0.1
  ))+
  ggtitle("Conversation Depth", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("Number messages in conversation")+
  ylab("Percentage")+theme_economist()

# filter message for Sep, Oct, Nov 2017 and count number of messages in thread by user type
dfStatsByUserType <- 
  dfMessages_L0 %>%
  filter(posted >= '2017-09-01-01 00:00:00' 
         & posted < '2017-12-01-01 00:00:00')%>%
  mutate(u_is_business = ifelse(u_is_business == 1, "business", "private")) %>%
  group_by(topics_count, u_is_business) %>%
  summarise(qtyTopicsCount = sum(n())) %>%
  group_by(u_is_business) %>%
  mutate(perTopicsCount = qtyTopicsCount / sum(qtyTopicsCount))

# plot it by user type --------------------------------------------------------
ggplot(dfStatsByUserType)+
  geom_bar(stat="identity", aes(x = topics_count, y = perTopicsCount, group = u_is_business, fill = u_is_business), position = "dodge")+
  scale_x_continuous(limits = c(0, 8), breaks = seq(1,8,1))+
  scale_y_continuous(limits = c(0, 1), labels = percent)+
  geom_label(
    aes(x = topics_count, 
        y = perTopicsCount, group = u_is_business, 
        label = percent(round(perTopicsCount, 3))), position = position_dodge(0.9), vjust = -1
  )+
  geom_text(
    aes(x = topics_count, 
        y = perTopicsCount, group = u_is_business,
        label = qtyTopicsCount, vjust = -0.1
    ), position = position_dodge(0.9))+
  ggtitle("Conversation Depth", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("Number messages in conversation")+
  ylab("Percentage")+theme_economist()

# search first answer of seller -----------------------------------------------
dfFirstAnswerSeller <-
  dfMessages %>%
  filter(parent_id %in% unique(dfMessages_L0$id)) %>%
  filter(sender_id == seller_id) %>%
  group_by(parent_id) %>%
  summarise(minId = min(id))

# 
dfConversations <- 
  dfMessages_L0[,
                c("id", "parent_id", "ad_id", "seller_id", "buyer_id", "sender_id",
                  "posted", "readed_at", "message","topics_count", "u_is_business")] %>%
  mutate(u_is_business = ifelse(u_is_business == 1, "business", "private")) %>%
  left_join(dfFirstAnswerSeller, by = c("id"="parent_id")) %>%
  left_join(dfMessages[,
                          c("id", "parent_id", "ad_id", "seller_id", "buyer_id", "sender_id",
                            "posted", "readed_at", "message")],
            by = c("minId"="id"))

dfConversationsStats <-
  dfConversations %>%
  mutate(AnswerFromSeller = ifelse(is.na(parent_id.y),"No","Yes")) %>%
  group_by(AnswerFromSeller) %>%
  summarise(qtyConversations = sum(n())) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsStats) +
  geom_bar(stat = "identity", aes(x = AnswerFromSeller, perConversations))+
  geom_text(aes(x = AnswerFromSeller, y = perConversations, label = qtyConversations), vjust = -0.1)+
  geom_label(aes(x = AnswerFromSeller, y = perConversations, label = percent(round(perConversations,3))), vjust = -1)+
  ylim(0, 0.8)+
  ggtitle("Do sellers answer to buyers using our site's message system?", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")+theme_economist()
  

dfConversationsStatsByUserType <-
  dfConversations %>%
  mutate(AnswerFromSeller = ifelse(is.na(parent_id.y),"No","Yes")) %>%
  group_by(AnswerFromSeller, u_is_business) %>%
  summarise(qtyConversations = sum(n())) %>%
  group_by(u_is_business) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsStatsByUserType) +
  geom_bar(stat = "identity", aes(x = AnswerFromSeller, y = perConversations, group=u_is_business, fill=u_is_business), position = "dodge")+
  geom_label(aes(x = AnswerFromSeller, y = perConversations, group = u_is_business, label = percent(round(perConversations, 3))), position = position_dodge(1), vjust = -1)+
  geom_text(aes(x = AnswerFromSeller, y = perConversations, group = u_is_business, label = qtyConversations), position = position_dodge(1), vjust = -0.1)+
  ylim(0, 1)+
  ggtitle("Do sellers answer to buyers using our site's message system?", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")+theme_economist()

dfConversationsReadStats <-
  dfConversations %>%
  filter(!is.na(readed_at.x)) %>%
  mutate(AnswerFromSeller = ifelse(is.na(parent_id.y),"No","Yes")) %>%
  group_by(AnswerFromSeller) %>%
  summarise(qtyConversations = sum(n())) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsReadStats) +
  geom_bar(stat = "identity", aes(x = AnswerFromSeller, perConversations))+
  geom_text(aes(x = AnswerFromSeller, y = perConversations, label = qtyConversations), vjust = -0.1)+
  geom_label(aes(x = AnswerFromSeller, y = perConversations, label = percent(round(perConversations,3))), vjust = -1)+
  ylim(0, 0.7)+
  ggtitle("If sellers read buyers' message, do they answer using our site's message system?", subtitle = "otomoto.pl 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")


dfConversationsReadedStats <-
  dfConversations %>%
  mutate(MessageRead = ifelse(is.na(readed_at.x),"Unread","Read")) %>%
  group_by(MessageRead) %>%
  summarise(qtyConversations = sum(n())) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsReadedStats) +
  geom_bar(stat = "identity", aes(x = MessageRead, perConversations))+
  geom_text(aes(x = MessageRead, y = perConversations, label = qtyConversations), vjust = -0.1)+
  geom_label(aes(x = MessageRead, y = perConversations, label = percent(round(perConversations,3))), vjust = -1)+
  ylim(0, 1)+
  ggtitle("Do sellers read buyers' messages?", subtitle = "autovit.ro 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")

dfConversationsReadHourStats <-
  dfConversations %>%
  filter(!is.na(readed_at.x)) %>%
  mutate(ReadHour = hour(readed_at.x)) %>%
  group_by(ReadHour) %>%
  summarise(qtyConversations = sum(n())) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsReadHourStats) +
  geom_line(stat = "identity", aes(x = ReadHour, perConversations))+
  #geom_text(aes(x = ReadHour, y = perConversations, label = qtyConversations), vjust = -0.1)+
  #geom_label(aes(x = ReadHour, y = perConversations, label = percent(round(perConversations,3))), vjust = -1)+
  scale_x_continuous(breaks = seq(0,23,1))+
  scale_y_continuous(labels = percent, limits = c(0, 0.1))+
  ggtitle("When do the sellers read the messages?", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")+theme_economist()

# -------------------------------------------------------------------------------
dfConversationsReadHourStatsByUserType <-
  dfConversations %>%
  filter(!is.na(readed_at.x)) %>%
  mutate(ReadHour = hour(readed_at.x)) %>%
  group_by(ReadHour, u_is_business) %>%
  summarise(qtyConversations = sum(n())) %>%
  group_by(u_is_business) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations))

ggplot(dfConversationsReadHourStatsByUserType) +
  geom_line(stat = "identity", aes(x = ReadHour, perConversations, color=u_is_business, group=u_is_business))+
  #geom_text(aes(x = ReadHour, y = perConversations, label = qtyConversations), vjust = -0.1)+
  #geom_label(aes(x = ReadHour, y = perConversations, label = percent(round(perConversations,3))), vjust = -1)+
  scale_x_continuous(breaks = seq(0,23,1))+
  scale_y_continuous(labels = percent, limits = c(0, 0.2))+
  ggtitle("When do the sellers read the messages?", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("")+ylab("percentage")+theme_economist()

# -------------------------------------------------------------------------------
dfConversationsTimeToAnswerStats <-
  dfConversations %>%
  filter(!is.na(readed_at.x) & !is.na(parent_id.y)) %>%
  mutate(TimeToAnswer = ceiling(difftime(posted.y, posted.x, units = "hour"))) %>%
  group_by(TimeToAnswer) %>%
  summarise(qtyConversations = sum(n())) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations)) %>% 
  mutate(cumPerConversations = cumsum(perConversations))

ggplot(
  dfConversationsTimeToAnswerStats[
    as.numeric(dfConversationsTimeToAnswerStats$TimeToAnswer) %in% c(1,2,4,6, 12, 18, 24, 48, 72, 96), ])+
  geom_point(stat="identity", aes(x = as.numeric(TimeToAnswer), y = cumPerConversations))+
  geom_line(stat="identity", aes(x = as.numeric(TimeToAnswer), y = cumPerConversations))+
  scale_x_continuous(breaks = c(1,2,4,6, 12, 18, 24, 48, 72, 96), limits = c(0, 100))+
  scale_y_continuous(breaks = seq(0,1,0.05), limits = c(0,1), labels = percent)+
  ggtitle("Sellers' answering time to buyers' first message", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("answering time (in hours - rounded up)") + ylab("Cumulative percentage")+
  theme_economist()


# -------------------------------------------------------------------------------
  dfConversationsTimeToAnswerStatsByUserType <-
  dfConversations %>%
  filter(!is.na(readed_at.x) & !is.na(parent_id.y)) %>%
  mutate(TimeToAnswer = ceiling(difftime(posted.y, posted.x, units = "hour"))) %>%
  group_by(TimeToAnswer, u_is_business) %>%
  summarise(qtyConversations = sum(n())) %>%
  group_by(u_is_business) %>%
  mutate(perConversations = qtyConversations / sum(qtyConversations)) %>% 
  mutate(cumPerConversations = cumsum(perConversations))

ggplot(
  dfConversationsTimeToAnswerStatsByUserType[
    as.numeric(dfConversationsTimeToAnswerStatsByUserType$TimeToAnswer) %in% c(1,2,4,6, 12, 18, 24, 48, 72, 96), ])+
  geom_point(stat="identity", aes(x = as.numeric(TimeToAnswer), y = cumPerConversations, color=u_is_business, group=u_is_business))+
  geom_line(stat="identity", aes(x = as.numeric(TimeToAnswer), y = cumPerConversations, color=u_is_business, group=u_is_business))+
  scale_x_continuous(breaks = c(1,2,4,6, 12, 18, 24, 48, 72, 96), limits = c(0, 100))+
  scale_y_continuous(breaks = seq(0,1,0.05), limits = c(0,1), labels = percent)+
  ggtitle("Sellers' answering time to buyers' first message", subtitle = "standvirtual.pt 01/Sep - 30/Nov/2017")+
  xlab("answering time (in hours - rounded up)") + ylab("Cumulative percentage")+
  theme_economist()

# ---- word cloud

dfWithAnswer <-
  dfConversations[!is.na(dfConversations$parent_id.y), ]

dfWithoutAnswer <-
  dfConversations[is.na(dfConversations$parent_id.y), ]

docsWithAnswer <- Corpus(VectorSource(dfWithAnswer$message.x))
docsWithoutAnswer <- Corpus(VectorSource(dfWithoutAnswer$message.x))

# Convert the text to lower case
docsWithAnswer <- tm_map(docsWithAnswer, content_transformer(tolower))
docsWithoutAnswer <- tm_map(docsWithoutAnswer, content_transformer(tolower))

# Remove your own stop word
# specify your stopwords as a character vector
freqWords <- c("pozdrawiam", "dobry", "nie", "się", "zł", "tys", "pana", "proszę",
               "dzień", "auto", "czy", "jest", "jestem", "witam")
docsWithAnswer <- tm_map(docsWithAnswer, removeWords, freqWords)
docsWithoutAnswer <- tm_map(docsWithoutAnswer, removeWords, freqWords)

# Text stemming
#docs <- tm_map(docs, stemDocument)

# Remove numbers
docsWithAnswer <- tm_map(docsWithAnswer, removeNumbers)
docsWithoutAnswer <- tm_map(docsWithoutAnswer, removeNumbers)

# Remove punctuations
docsWithAnswer <- tm_map(docsWithAnswer, removePunctuation)
docsWithoutAnswer <- tm_map(docsWithoutAnswer, removePunctuation)

# Eliminate extra white spaces
docsWithAnswer <- tm_map(docsWithAnswer, stripWhitespace)
docsWithoutAnswer <- tm_map(docsWithoutAnswer, stripWhitespace)

dtmWithAnswer <- TermDocumentMatrix(docsWithAnswer)
dtmWithoutAnswer <- TermDocumentMatrix(docsWithoutAnswer)

#remover termos mais frequentes
dtmWithAnswer <- removeSparseTerms(dtmWithAnswer, sparse=0.95)
dtmWithoutAnswer <- removeSparseTerms(dtmWithoutAnswer, sparse=0.95)

ap.m <- as.matrix(dtmWithAnswer)
ap.v <- sort(rowSums(ap.m),decreasing=TRUE)
ap.d <- data.frame(word = names(ap.v),freq=ap.v)
table(ap.d$freq)
pal2 <- brewer.pal(8,"Dark2")
png("dtmWithAnswer.png", width=1280,height=800)
wordcloud(ap.d$word,ap.d$freq, scale=c(8,.2),min.freq=3,
          max.words=Inf, random.order=FALSE, rot.per=.15, colors=pal2)
dev.off()


ap.m <- as.matrix(dtmWithoutAnswer)
ap.v <- sort(rowSums(ap.m),decreasing=TRUE)
ap.d <- data.frame(word = names(ap.v),freq=ap.v)
#table(ap.d$freq)
pal2 <- brewer.pal(8,"Dark2")
png("dtmWithoutAnswer.png", width=1280,height=800)
wordcloud(ap.d$word,ap.d$freq, scale=c(8,.2),min.freq=3,
          max.words=Inf, random.order=FALSE, rot.per=.15, colors=pal2)
dev.off()



#remover termos mais frequentes
dtmWithAnswer <- removeSparseTerms(dtmWithAnswer, sparse=0.95)
dtmWithoutAnswer <- removeSparseTerms(dtmWithoutAnswer, sparse=0.95)

matrixDtmWithAnswer <- as.data.frame(inspect(dtmWithAnswer))
matrixDtmWithoutAnswer <- as.data.frame(inspect(dtmWithoutAnswer))

mydata.df <- as.data.frame(inspect(docs.dtm2))

findAssocs(docs.dtm2, 'dobry', 0.20) 

findFreqTerms(dtmWithAnswer, 10)

head(dfConversations$message.x[grepl(".*kontakt.*", dfConversations$message.x)], 10)