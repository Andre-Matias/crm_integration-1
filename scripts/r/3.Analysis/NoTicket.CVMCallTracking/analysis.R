library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("stringr")

source("~/verticals-bi/scripts/r/3.Analysis/NoTicket.CVMCallTracking/extractParameters.R")

dfAds <-
  as_tibble(readRDS("~/CT/dfAds_StandvirtualPT.RDS"))

t <- as_tibble(dfAds[, c("id", "params")])

colnames(t) <- c("ad_id", "params")

t0 <-
  t %>%
  unnest(params = strsplit(params, "<br>")) %>%
  mutate(new = strsplit(params, "<=>"),
         length = lapply(new, length)) %>%
  filter(length >=2 ) %>%
  mutate(paramName = unlist(lapply(new, function(x) x[1])),
         paramValue = unlist(lapply(new, function(x) x[2]))
  ) %>%
  select(ad_id, paramName, paramValue) %>%
  filter(paramName %in% c("make", "model"))

t0[t0$paramName=="price" & !is.na(suppressWarnings(as.numeric(t0$paramValue))), c("paramName")] <- "priceValue"

t0$paramName <- gsub("price\\[currency\\]", "price_currency", t0$paramName, perl = TRUE)
t0$paramName <- gsub("price\\[gross_net\\]", "price_gross_net", t0$paramName, perl = TRUE)

dfParams <-
  t0 %>%
  filter(paramName != "features") %>%
  group_by(ad_id, paramName) %>%
  summarise(paramValue = max(paramValue)) %>%
  spread(key = paramName, value = paramValue)


dfUsers <-
  as_tibble(readRDS("~/CT/dfUsers_StandvirtualPT.RDS"))

dfStands <-
  as_tibble(readRDS("~/CT/dfStands_StandvirtualPT.RDS"))

dfAds <- 
  dfAds %>%
  filter(user_id %in% dfUsers$id) %>%
  inner_join(dfParams, by = c("id" = "ad_id")) %>%
  select(-params)

df <-
  dfAds %>%
  group_by(user_id) %>%
  summarise(qtyAds = sum(n()))

set.seed(20)
usersCluster <- kmeans(df[, 2], 3, nstart = 20)

df$cluster <- usersCluster$cluster

dfClusters <-
  df %>% 
  group_by(cluster) %>%
  summarise(
    qtyUsers = sum(n()),
    minCluster = min(qtyAds),
    maxCluster = max(qtyAds),
    qtyAds = sum(qtyAds)
  )

dfAdsStands <-
  dfAds %>%
  filter(!is.na(stand_id)) %>%
  inner_join(dfStands, by = c("stand_id" = "id")) %>%
  mutate(user_id = user_id.x) %>%
  select(id, created_at_first, make, model, user_id, phone1, phone2, phone3) 

dfAdsUsers <-
  dfAds %>%
  filter(is.na(stand_id)) %>%
  inner_join(dfUsers, by = c("user_id" = "id")) %>%
  select(id, created_at_first, make, model, user_id, phone1, phone2, phone3) 

dfAdsPhones <-
  rbind(dfAdsStands, dfAdsUsers)

dfAdsPhones$Mobile1 <- grepl("^9.*", dfAdsPhones$phone1)
dfAdsPhones$Mobile2 <- grepl("^9.*", dfAdsPhones$phone2)
dfAdsPhones$Mobile3 <- grepl("^9.*", dfAdsPhones$phone3)

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
  filter(make %in% c("renault", "bmw", "mercedes-benz", "peugeot", "vw")) %>%
  filter(model %in% c("megane-sport-tourer")) %>%
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
  


