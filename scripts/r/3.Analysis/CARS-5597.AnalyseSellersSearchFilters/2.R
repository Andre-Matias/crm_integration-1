library("arules")
library("data.table")
library("dplyr")
library("dtplyr")

d <-
  readRDS("~/d_stv_buyers.RDS")

d <- d[!(d$L2 %in% c('item_condition', 'damage')), ]

d$L1 <- as.factor(as.character(d$L1))
d$L2 <- as.factor(as.character(d$L2))

d <- as_tibble(d)

d1 <-
  d %>%
  group_by(L1) %>%
  summarise(L3 = paste(L2, collapse=", "))
d1$L1 <- as.factor(as.character(d1$L1))
d1$L3 <- as.factor(as.character(d1$L3))
d1 <- d1[, "L3"]

txn <- as(d1, "transactions")

basket_rules <- apriori(d1, parameter = list(sup = 0.005, conf = 0.01, target="rules"))

z <-as(basket_rules, "data.frame")

