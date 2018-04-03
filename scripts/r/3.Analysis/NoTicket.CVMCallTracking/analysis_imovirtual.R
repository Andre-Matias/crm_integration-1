library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("stringr")

dfAds <-
  as_tibble(readRDS("~/CT/dfAds_ImovirtualPT.RDS"))

dfUsers <-
  as_tibble(readRDS("~/CT/dfUsers_ImovirtualPT.RDS"))

dfAgents <-
  as_tibble(readRDS("~/CT/dfAgents_ImovirtualPT.RDS"))

# split phone field in Ads Table
dfAds$phoneList <- lapply(dfAds$phone, function(x) str_split(x, pattern = ",|;"))

dfAds$phoneListSize <- lapply(dfAds$phoneList, function(x) lengths(x))

dfAds$phone1 <- ""
dfAds$phone2 <- ""
dfAds$phone3 <- ""

dfAds$phone1[dfAds$phoneListSize >= 1] <- 
  lapply(dfAds$phone[dfAds$phoneListSize >= 1], function(x) unlist(str_split(x, pattern = ",|;")[[1]][1]))

dfAds$phone2[dfAds$phoneListSize >= 2] <- 
  lapply(dfAds$phone[dfAds$phoneListSize >= 2], function(x) unlist(str_split(x, pattern = ",|;")[[1]][2]))

dfAds$phone3[dfAds$phoneListSize >= 3] <- 
  lapply(dfAds$phone[dfAds$phoneListSize >= 3], function(x) unlist(str_split(x, pattern = ",|;")[[1]][3]))


dfAds$phone1 <-
  gsub("\\+351", "", dfAds$phone1)
dfAds$phone2 <-
  gsub("\\+351", "", dfAds$phone2)
dfAds$phone3 <-
  gsub("\\+351", "", dfAds$phone3)

dfAds$phone1 <-
  gsub("\\(|\\)", "", dfAds$phone1)
dfAds$phone2 <-
  gsub("\\(|\\)", "", dfAds$phone2)
dfAds$phone3 <-
  gsub("\\(|\\)", "", dfAds$phone3)

dfAds$phone1 <-
  gsub("\\+", "", dfAds$phone1)
dfAds$phone2 <-
  gsub("\\+", "", dfAds$phone2)
dfAds$phone3 <-
  gsub("\\+", "", dfAds$phone3)

dfAds$phone1 <-
  gsub("\\-", "", dfAds$phone1)
dfAds$phone2 <-
  gsub("\\-", "", dfAds$phone2)
dfAds$phone3 <-
  gsub("\\-", "", dfAds$phone3)

dfAds$phone1 <-
  gsub("/", "", dfAds$phone1)
dfAds$phone2 <-
  gsub("/", "", dfAds$phone2)
dfAds$phone3 <-
  gsub("/", "", dfAds$phone3)

dfAds$phone1 <-
  gsub(" ", "", dfAds$phone1)
dfAds$phone2 <-
  gsub(" ", "", dfAds$phone2)
dfAds$phone3 <-
  gsub(" ", "", dfAds$phone3)


# remove columns from Ads Table
dfAds$phoneList <- NULL
dfAds$phoneListSize <- NULL

# Agents Table ----------------------------------------------------------------

# remove contry code from phone
dfAgents$phone <-
  gsub("\\+351", "", dfAgents$phone)

# remove parenthesis from phone
dfAgents$phone <-
  gsub("\\(|\\)", "", dfAgents$phone)

dfAgents$phone <-
  gsub("\\+", "", dfAgents$phone)

dfAgents$phone <-
  gsub("\\+ ", "", dfAgents$phone)

dfAgents$phone <-
  gsub("\\-", "", dfAgents$phone)

dfAgents$phone <-
  gsub("/", "", dfAgents$phone)

dfAgents$phone <-
  gsub(" ", "", dfAgents$phone)

dfAgents$phone <-
  gsub("[a-z]", "", dfAgents$phone)

dfAgents$phone <-
  gsub("\\.", "", dfAgents$phone)

dfAgents$phone <-
  gsub(",", "", dfAgents$phone)

dfAgents$phonelength <- lapply(dfAgents$phone, function(x) nchar(x))

dfAgents$phone1 <- ""
dfAgents$phone2 <- ""
dfAgents$phone3 <- ""

dfAgents$phoneList <- lapply(dfAgents$phone, function(x) str_split(x, pattern = ",|;"))

dfAgents$phoneListSize <- lapply(dfAgents$phoneList, function(x) lengths(x))

dfAgents$phone1[dfAgents$phoneListSize >= 1] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 1], function(x) unlist(str_split(x, pattern = ",|;")[[1]][1]))

dfAgents$phone2[dfAgents$phoneListSize >= 2] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 2], function(x) unlist(str_split(x, pattern = ",|;")[[1]][2]))

dfAgents$phone3[dfAgents$phoneListSize >= 3] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 3], function(x) unlist(str_split(x, pattern = ",|;")[[1]][3]))

# -----------------------------------------------------------------------------

dfAdsAgents <-
  dfAds %>%
  filter(!is.na(agent_id) & agent_id != 0) %>%
  inner_join(dfAgents, by = c("agent_id" = "id")) %>%
  mutate(user_id = user_id.x) %>%
  mutate(phone1 = phone1.y,
         phone2 = phone2.y,
         phone3 = phone3.y
         
  )%>%
  select(id, created_at_first, category_id, agent_id, user_id, phone1, phone2, phone3)

dfAdsUsers <-
  dfAds %>%
  filter(is.na(agent_id) | agent_id == 0 ) %>%
  inner_join(dfUsers, by = c("user_id" = "id")) %>%
  mutate(phone1 = phone1.y,
         phone2 = phone2.y,
         phone3 = phone3.y
         
  )%>%
  select(id, created_at_first, category_id, agent_id, user_id, phone1, phone2, phone3)

dfAdsPhones <-
  rbind(dfAdsAgents, dfAdsUsers)

dfAdsAds <-
  dfAdsPhones %>%
  filter(phone1 == "" | is.na(phone1)) %>%
  select(id, created_at_first, category_id, agent_id, user_id) %>%
  inner_join(dfAds[, c("id", "phone1", "phone2", "phone3")], by = c("id"="id"))

dfAdsPhones <-
  rbind(dfAdsPhones[dfAdsPhones$phone1 != "" & !is.na(dfAdsPhones$phone1), ], dfAdsAds)

dfAdsPhones$Mobile1 <- grepl("^9", dfAdsPhones$phone1)
dfAdsPhones$Mobile2 <- grepl("^9", dfAdsPhones$phone2)
dfAdsPhones$Mobile3 <- grepl("^9", dfAdsPhones$phone3)

dfAdsPhones$Mobile1[dfAdsPhones$phone1==""] <- NA
dfAdsPhones$Mobile2[dfAdsPhones$phone2==""] <- NA
dfAdsPhones$Mobile3[dfAdsPhones$phone3==""] <- NA

dfLiquidy7 <-
  dfAdsPhones %>%
  select(id, created_at_first) %>%
  mutate(created_at_first = as.POSIXct(created_at_first)) %>%
  inner_join(dfLeadsByAd, by = c("id"="item_id")) %>%
  filter(eventname %in% c("reply_phone_call", "reply_phone_sms")) %>% #, "reply_message_sent", "reply_chat_sent", "reply_phone_show")) %>%
  filter(date < created_at_first + days(7)) %>%
  group_by(id)%>%
  summarise(qtyEvents = sum(qtyEvents, na.rm = TRUE)) #%>%
  #spread(key = eventname, value = qtyEvents, fill = 0)


dfFinal7 <-
  dfAdsPhones %>%
  left_join(dfLiquidy7, by = c("id" = "id")) %>%
  mutate(isLiquid = !is.na(qtyEvents)) %>%
  group_by(Mobile1, Mobile2) %>%
  summarise(qtyIsLiquid = sum(isLiquid == T),
            qtyIsNotLiquid = sum(isLiquid == F)
            ) %>%
  mutate(Liquidity = qtyIsLiquid / (qtyIsLiquid + qtyIsNotLiquid))


dfLiquidy14 <-
  dfAdsPhones %>%
  select(id, created_at_first) %>%
  mutate(created_at_first = as.POSIXct(created_at_first)) %>%
  inner_join(dfLeadsByAd, by = c("id"="item_id")) %>%
  filter(eventname %in% c("reply_phone_call", "reply_phone_sms")) %>% #, "reply_message_sent", "reply_chat_sent", "reply_phone_show")) %>%
  filter(date < created_at_first + days(14)) %>%
  group_by(id)%>%
  summarise(qtyEvents = sum(qtyEvents, na.rm = TRUE)) #%>%
#spread(key = eventname, value = qtyEvents, fill = 0)


dfFinal14 <-
  dfAdsPhones %>%
  left_join(dfLiquidy14, by = c("id" = "id")) %>%
  mutate(isLiquid = !is.na(qtyEvents)) %>%
  group_by(Mobile1, Mobile2) %>%
  summarise(qtyIsLiquid = sum(isLiquid == T),
            qtyIsNotLiquid = sum(isLiquid == F)
  ) %>%
  mutate(Liquidity = qtyIsLiquid / (qtyIsLiquid + qtyIsNotLiquid))

dfLiquidy21 <-
  dfAdsPhones %>%
  select(id, created_at_first) %>%
  mutate(created_at_first = as.POSIXct(created_at_first)) %>%
  inner_join(dfLeadsByAd, by = c("id"="item_id")) %>%
  filter(eventname %in% c("reply_phone_call", "reply_phone_sms")) %>% #, "reply_message_sent", "reply_chat_sent", "reply_phone_show")) %>%
  filter(date < created_at_first + days(21)) %>%
  group_by(id)%>%
  summarise(qtyEvents = sum(qtyEvents, na.rm = TRUE)) #%>%
#spread(key = eventname, value = qtyEvents, fill = 0)


dfFinal21 <-
  dfAdsPhones %>%
  left_join(dfLiquidy21, by = c("id" = "id")) %>%
  mutate(isLiquid = !is.na(qtyEvents)) %>%
  group_by(Mobile1, Mobile2) %>%
  summarise(qtyIsLiquid = sum(isLiquid == T),
            qtyIsNotLiquid = sum(isLiquid == F)
  ) %>%
  mutate(Liquidity = qtyIsLiquid / (qtyIsLiquid + qtyIsNotLiquid))

