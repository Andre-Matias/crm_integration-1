library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("stringr")

dfAds <-
  as_tibble(readRDS("~/CT/dfAds_OtodomPL.RDS"))

dfUsers <-
  as_tibble(readRDS("~/CT/dfUsers_OtodomPL.RDS"))

dfAgents <-
  as_tibble(readRDS("~/CT/dfAgents_OtodomPL.RDS"))

# remove contry code from phone
dfAgents$phone <-
  gsub("\\+48", "", dfAgents$phone)

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
  gsub("^0", "", dfAgents$phone)

dfAgents$phone <-
  gsub("[a-z]", "", dfAgents$phone)

dfAgents$phone <-
  gsub("\\.", "", dfAgents$phone)

dfAgents$phoneList <- lapply(dfAgents$phone, function(x) str_split(x, pattern = ",|;"))

dfAgents$phoneListSize <- lapply(dfAgents$phoneList, function(x) lengths(x))

dfAgents$phone1 <- ""
dfAgents$phone2 <- ""
dfAgents$phone3 <- ""

dfAgents$phone1[dfAgents$phoneListSize >= 1] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 1], function(x) unlist(str_split(x, pattern = ",|;")[[1]][1]))

dfAgents$phone2[dfAgents$phoneListSize >= 2] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 2], function(x) unlist(str_split(x, pattern = ",|;")[[1]][2]))

dfAgents$phone3[dfAgents$phoneListSize >= 3] <- 
  lapply(dfAgents$phone[dfAgents$phoneListSize >= 3], function(x) unlist(str_split(x, pattern = ",|;")[[1]][3]))

dfAgents$phone1 <-
  gsub("^0", "", dfAgents$phone1)

dfAgents$phone2 <-
  gsub("^0", "", dfAgents$phone2)

dfAgents$phone3 <-
  gsub("^0", "", dfAgents$phone3)

dfAgents$phone1[nchar(dfAgents$phone1)==11] <-
  gsub("^48", "", dfAgents$phone1[nchar(dfAgents$phone1)==11])

dfAgents$phone2[nchar(dfAgents$phone2)==11] <-
  gsub("^48", "", dfAgents$phone2[nchar(dfAgents$phone2)==11])

dfAgents$phone3[nchar(dfAgents$phone3)==11] <-
  gsub("^48", "", dfAgents$phone3[nchar(dfAgents$phone3)==11])

dfAgents <- 
  dfAgents[nchar(dfAgents$phone1)==9, ]

dfAgents <- 
  dfAgents[nchar(dfAgents$phone2) == 9 | nchar(dfAgents$phone2) %in% c(0,9) | nchar(dfAgents$phone3) %in% c(9), ]

dfAdsAgents <-
  dfAds %>%
  filter(!is.na(agent_id)) %>%
  inner_join(dfAgents, by = c("agent_id" = "id")) %>%
  mutate(user_id = user_id.x) %>%
  select(id, created_at_first, category_id, user_id, phone1, phone2, phone3) 

dfAdsUsers <-
  dfAds %>%
  filter(is.na(agent_id)) %>%
  inner_join(dfUsers, by = c("user_id" = "id")) %>%
  select(id, created_at_first, category_id, user_id, phone1, phone2, phone3) 

dfAdsPhones <-
  rbind(dfAdsAgents, dfAdsUsers)

dfAdsPhones$Mobile1 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone1)
dfAdsPhones$Mobile2 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone2)
dfAdsPhones$Mobile3 <- grepl("^45|^50|^51|^53|^57|^60|^66|^69|^72|^73|^78|^79|^88", dfAdsPhones$phone3)

dfAdsPhones$Mobile1[dfAdsPhones$phone1==""] <- NA
dfAdsPhones$Mobile2[dfAdsPhones$phone2==""] <- NA
dfAdsPhones$Mobile3[dfAdsPhones$phone3==""] <- NA

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


dfFinal <-
  dfAdsPhones %>%
  left_join(dfLiquidy14, by = c("id" = "id")) %>%
  mutate(isLiquid = !is.na(qtyEvents)) %>%
  group_by(Mobile1, Mobile2) %>%
  summarise(qtyIsLiquid = sum(isLiquid == T),
            qtyIsNotLiquid = sum(isLiquid == F)
            ) %>%
  mutate(Liquidity = qtyIsLiquid / (qtyIsLiquid + qtyIsNotLiquid))

dfMakeModelStats <-
  dfAdsPhones %>%
  group_by(make, model) %>%
  summarise(qtyAds = sum(n())) %>%
  arrange(-qtyAds)
  


