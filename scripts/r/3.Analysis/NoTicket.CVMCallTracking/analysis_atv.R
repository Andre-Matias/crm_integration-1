library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("stringr")

dfAds <-
  as_tibble(readRDS("~/CT/dfAds_AutovitRO.RDS"))

dfUsers <-
  as_tibble(readRDS("~/CT/dfUsers_AutovitRO.RDS"))

dfAdsPhones <-
  dfAds %>%
  inner_join(dfUsers, by = c("user_id" = "id")) %>%
  select(id, created_at_first, user_id, phone1, phone2, phone3)

# remove spaces ---------------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub(" ", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub(" ", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub(" ", "", dfAdsPhones$phone3)

# remove dash ---------------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("-", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("-", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("-", "", dfAdsPhones$phone3)

# remove country code ---------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("\\+40", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("\\+40", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("\\+40", "", dfAdsPhones$phone3)

# remove country code ---------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("^0040", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("^0040", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("^0040", "", dfAdsPhones$phone3)

# remove pljus signal  ---------------------------------------------------------
dfAdsPhones$phone1 <-
  gsub("\\+", "", dfAdsPhones$phone1)

dfAdsPhones$phone2 <-
  gsub("\\+", "", dfAdsPhones$phone2)

dfAdsPhones$phone3 <-
  gsub("\\+", "", dfAdsPhones$phone3)

# remove parenthesis ----------
dfAdsPhones$phone1 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone1)
dfAdsPhones$phone2 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone2)
dfAdsPhones$phone3 <-
  gsub("\\(|\\)", "", dfAdsPhones$phone3)

dfAdsPhones$Mobile1 <- ""
dfAdsPhones$Mobile2 <- ""
dfAdsPhones$Mobile3 <- ""

dfAdsPhones$Mobile1 <- grepl("^07.*", dfAdsPhones$phone1)
dfAdsPhones$Mobile2 <- grepl("^07.*", dfAdsPhones$phone2)
dfAdsPhones$Mobile3 <- grepl("^07.*", dfAdsPhones$phone3)

dfAdsPhones$Mobile1[dfAdsPhones$phone1==""] <- NA
dfAdsPhones$Mobile2[dfAdsPhones$phone2==""] <- NA
dfAdsPhones$Mobile3[dfAdsPhones$phone3==""] <- NA

dfLiquidy <-
  dfAdsPhones %>%
  select(id, created_at_first) %>%
  mutate(created_at_first = as.POSIXct(created_at_first)) %>%
  inner_join(dfLeadsByAd, by = c("id"="item_id")) %>%
  filter(eventname %in% c("reply_phone_call", "reply_phone_sms")) %>% #, "reply_message_sent", "reply_chat_sent", "reply_phone_show")) %>%
  filter(date < created_at_first + days(7)) %>%
  group_by(id)%>%
  summarise(qtyEvents = sum(qtyEvents, na.rm = TRUE)) #%>%
  #spread(key = eventname, value = qtyEvents, fill = 0)


dfFinal <-
  dfAdsPhones %>%
  left_join(dfLiquidy, by = c("id" = "id")) %>%
  mutate(isLiquid = !is.na(qtyEvents)) %>%
  group_by(Mobile1, Mobile2) %>%
  summarise(qtyIsLiquid = sum(isLiquid == T),
            qtyIsNotLiquid = sum(isLiquid == F)
            ) %>%
  mutate(Liquidity = qtyIsLiquid / (qtyIsLiquid + qtyIsNotLiquid))