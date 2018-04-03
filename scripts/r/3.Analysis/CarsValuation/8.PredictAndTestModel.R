library("ranger")
library("scales")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("hydroGOF")


options(scipen=9999)

load("~/tmp/RF_model.rda")

dfInputForModel_test <- readRDS("~/tmp/dfInputForModel_test.RDS")

p <- predict(RF_model, data = dfInputForModel_test)

dfInputForModel_test$p <- p$predictions


dfInputForModel_test$errorA <- 
  abs(dfInputForModel_test$price_PLN - dfInputForModel_test$p)

dfInputForModel_test$error <-
  dfInputForModel_test$errorA / dfInputForModel_test$price_PLN

b <- dfInputForModel_test %>% arrange(error)

a <- gof(dfInputForModel_test$price_PLN, dfInputForModel_test$p)

#saveRDS(dfInputForModel_test, "~/tmp/dfInputForModel_test_fit.RDS")


dfStats <-
  dfInputForModel_test %>%
  group_by(error) %>%
  summarise(qtyAds = sum(n())) %>%
  mutate(perQtyAds = qtyAds / sum(qtyAds)) %>%
  arrange(error)%>%
  group_by() %>%
  arrange(error)%>%
  mutate(cumQtyAds = cumsum(perQtyAds))

head(df %>% arrange(desc(qtyAds)))