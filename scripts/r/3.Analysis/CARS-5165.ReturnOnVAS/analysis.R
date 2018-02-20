#load libraries
library("aws.s3")
library("feather")
library("dplyr")
library("data.table")
library("dtplyr")
library("fasttime")
library("magrittr")
library("ggplot2")
library("parallel")
library("tidyr")

options(scipen = 9999)

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

df <-
  as_tibble(readRDS("~/RawHistoricalAds_OTO_main_AIO_wPrice.RDS"))

dfActiveTimeByDay <-
  as_tibble(
    s3readRDS(
      object = "CARS-5165/ActiveTimeByDay.RDS",
      bucket = s3BucketName)
  )

dfVASByAdIdType <-
  as_tibble(
    s3readRDS(
      object = "CARS-5165/dfVASByAdIdType.RDS",
      bucket = s3BucketName)
  )

dfRepliesByAdID <-
  as_tibble(
    s3readRDS(
      object = "CARS-5165/dfRepliesByAdID.RDS",
      bucket = s3BucketName)
  )

dfActiveTimeByDay$ad_id <- as.numeric(dfActiveTimeByDay$id)

df2 <-
  df %>%
  select(ad_id, created_at_first_date, make, model, year, mileage, engine_power, engine_capacity, price_PLN) %>%
  inner_join(dfRepliesByAdID) %>%
  inner_join(dfActiveTimeByDay) %>%
  select(-id) %>%
  mutate(qtyAdImpressionsByActiveDay = qtyAdImpressions / qtyTimeLiveInDays,
            qtyAdPageLoadsByActiveDay = qtyAdPageLoads / qtyTimeLiveInDays,
            qtyShowPhoneByActiveDay = qtyShowPhone / qtyTimeLiveInDays,
            qtyAdMessagesByActiveDay = qtyAdMessages / qtyTimeLiveInDays
  ) %>%
  inner_join(dfVASByAdIdType)

df2$ad_bighomepage[is.na(df2$ad_bighomepage)] <-0
df2$ad_homepage[is.na(df2$ad_homepage)] <-0
df2$highlight[is.na(df2$highlight)] <-0
df2$export_olx[is.na(df2$export_olx)] <-0
df2$topads[is.na(df2$topads)] <- 0
df2$bump_up[is.na(df2$bump_up)] <-0
df2$paid_for_post[is.na(df2$paid_for_post)] <-0


df2$ad_bighomepage <- as.numeric(df2$ad_bighomepage)
df2$ad_homepage <- as.numeric(df2$ad_homepage)
df2$highlight <- as.numeric(df2$highlight)
df2$export_olx <- as.numeric(df2$export_olx)
df2$topads <- as.numeric(df2$topads)
df2$bump_up <- as.numeric(df2$bump_up)
df2$paid_for_post <- as.numeric(df2$paid_for_post)

# -----------------------------------------------------------------------------
dfAll <- 
  df2 %>% 
  gather(key = "VAS", value = "VAS_value",  19:25)%>%
  filter(
    # outliers
    # price
    price_PLN < 207950, price_PLN > 2000,
    # year
    year > 2008, year < 2018,
  #
  qtyAdImpressionsByActiveDay < 15000,
  qtyAdPageLoadsByActiveDay < 1000,
  qtyShowPhoneByActiveDay < 10,
  qtyAdMessagesByActiveDay < 10
  ) %>%
  filter(VAS!='paid_for_post', VAS_value %in% c(0,1)) %>%
  mutate(VAS_value = ifelse(VAS_value==0, "without", "with"))%>%
  group_by(make, model, VAS, VAS_value) %>%
  summarise(meanPrice_PLN = mean(as.numeric(price_PLN)),
            meanYear = mean(as.numeric(year)),
            qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay), 
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
            ) %>%
  group_by(make, model, VAS) %>%
  mutate(m = sum(qtyAds)) %>%
  gather(key = "Output", value = "Output_value",  5:12) %>%
  unite("VAS_value_Output", c("VAS_value","Output"), sep = "_") %>%
  spread(key = "VAS_value_Output", value = "Output_value", fill = 0) %>%
  mutate(varAvgTimePlatform = 
           with_avgTimePlatform / without_avgTimePlatform - 1,
         varAvgAdImpressionsByActiveDay = 
           with_avgAdImpressionsByActiveDay / without_avgAdImpressionsByActiveDay - 1,
         varAvgAdPageLoadsByActiveDay = 
           with_avgAdPageLoadsByActiveDay / without_avgAdPageLoadsByActiveDay - 1,
         varAvgShowPhoneByActiveDay = 
           with_avgShowPhoneByActiveDay / without_avgShowPhoneByActiveDay - 1,
         varAvgAdMessagesByActiveDay = 
           with_avgAdMessagesByActiveDay / without_avgAdMessagesByActiveDay - 1) %>%
  filter(without_qtyAds > 2, with_qtyAds >2 )%>%
  mutate(powerAdImpressions =
           pwr.2p2n.test(h=varAvgAdImpressionsByActiveDay,
                       n1=without_qtyAds,
                       n2=with_qtyAds,
                       sig.level=0.05,
                       alternative="two.sided")$power,
         powerPageLoads =
           pwr.2p2n.test(h=varAvgAdPageLoadsByActiveDay,
                         n1=without_qtyAds,
                         n2=with_qtyAds,
                         sig.level=0.05,
                         alternative="two.sided")$power,
         powerShowPhone =
           pwr.2p2n.test(h=varAvgShowPhoneByActiveDay,
                         n1=without_qtyAds,
                         n2=with_qtyAds,
                         sig.level=0.05,
                         alternative="two.sided")$power,
         powerMessages =
           pwr.2p2n.test(h=varAvgAdMessagesByActiveDay,
                         n1=without_qtyAds,
                         n2=with_qtyAds,
                         sig.level=0.05,
                         alternative="two.sided")$power
         
  )
  
dfPower <- 
  dfAll %>%
  mutate(powerAdImpressions = ifelse(powerAdImpressions >= 0.80, "Valid", "notValid"),
         powerPageLoads = ifelse(powerPageLoads >= 0.80, "Valid", "notValid"),
         powerShowPhone = ifelse(powerShowPhone >= 0.80,"Valid", "notValid"),
         powerMessages = ifelse(powerMessages >= 0.80, "Valid", "notValid")
  ) %>%
  select(make, model, VAS, m, powerAdImpressions, powerPageLoads, powerShowPhone, powerMessages) %>%
  gather(key = testPower, value = testValue, 5:8) %>%
  group_by(VAS, testPower, testValue) %>%
  summarise(qty = sum(n()), qtyAds = sum(m)) %>%
  mutate(per = qty/sum(qty)) %>%
  spread(key = testValue, value = c("per")) %>%
  group_by(VAS, testPower) %>%
  summarise(qty = sum(qty, na.rm = TRUE), 
            perValid = sum(Valid, na.rm = TRUE), 
            perNotValid = sum(notValid, na.rm = TRUE),
            qtyAds = sum(qtyAds, na.rm = TRUE)
            )
  

# -----------------------------------------------------------------------------
statsOpelAstra <- 
  df2 %>% 
  filter(make == 'opel',  model=="astra") %>%
  group_by(make, model, highlight) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
  )

ggplot(statsOpelAstra) +
  geom_bar(stat="identity", 
           aes(x=highlight, y=avgTimePlatform)
           )+
  scale_x_continuous(breaks=seq(0,10,1))+
  ggtitle("Average Days On Platform", subtitle = "Opel Astra")

ggplot(statsOpelAstra) +
  geom_bar(stat="identity", 
           aes(x=highlight, y=avgAdImpressionsByActiveDay)
  )+
  scale_x_continuous(breaks=seq(0,10,1))+
  ggtitle("Ad Impressions", subtitle = "Opel Astra")

ggplot(statsOpelAstra) +
  geom_bar(stat="identity", 
           aes(x=highlight, y=avgAdPageLoadsByActiveDay)
  )+
  scale_x_continuous(breaks=seq(0,10,1))+
  ggtitle("Ad Page Loads", subtitle = "Opel Astra")

ggplot(statsOpelAstra) +
  geom_bar(stat="identity", 
           aes(x=highlight, y=avgShowPhoneByActiveDay)
  )+
  scale_x_continuous(breaks=seq(0,10,1))+
  ggtitle("Show Phone", subtitle = "Opel Astra")

ggplot(statsOpelAstra) +
  geom_bar(stat="identity", 
           aes(x=highlight, y=avgAdMessagesByActiveDay)
  )+
  scale_x_continuous(breaks=seq(0,10,1))+
  ggtitle("Messages", subtitle = "Opel Astra")

















statsVWGolf <- 
  df2 %>% 
  filter(make == 'volkswagen',  model=="golf") %>%
  group_by(make, model, export_olx) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
            )

statsVWPassat <- 
  df2 %>% 
  filter(make == 'volkswagen',  model=="passat") %>%
  group_by(make, model, export_olx) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
  )


statsAudiA4 <- 
  df2 %>% 
  filter(make == 'audi',  model=="a4") %>%
  group_by(make, model, export_olx) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
  )

statsFordFocus <- 
  df2 %>% 
  filter(make == 'ford',  model=="focus") %>%
  group_by(make, model, export_olx) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
  )

statsBMWSeries3 <- 
  df2 %>% 
  filter(make == 'bmw',  model=="seria-3") %>%
  group_by(make, model, export_olx) %>%
  summarise(qtyAds = sum(n()),
            avgTimePlatform = mean(qtyTimeLiveInDays), 
            avgAdImpressionsByActiveDay = mean(qtyAdImpressionsByActiveDay),
            avgAdPageLoadsByActiveDay = mean(qtyAdPageLoadsByActiveDay),
            avgShowPhoneByActiveDay = mean(qtyShowPhoneByActiveDay),
            avgAdMessagesByActiveDay =  mean(qtyAdMessagesByActiveDay)
  )

cor(as.numeric(dfAll[dfAll$VAS=="ad_bighomepage" & dfAll$VAS_value==1, c("VAS_value")]),
    as.numeric(dfAll[dfAll$VAS=="ad_bighomepage" & dfAll$VAS_value==1, c("qtyAdMessagesByActiveDay")]))
