library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")

#"USE calltrackingdb; SELECT service_id, external_user_id, external_sub_user_id, call_result, COUNT(*) FROM call_statistics WHERE service_id = 1 GROUP BY 1,2,3,4;"
# "USE calltrackingdb; SELECT * FROM call_statistics" > dump_call_statistics.txt

dfRawCallStatistics <-
  as_tibble(read.table("~/dump_call_statistics.txt", header = TRUE, sep = "\t"))

dfRawService <-
  as_tibble(read.table("~/dump_service.txt", header = TRUE, sep = "\t"))

Stats <-
  dfRawCallStatistics %>%
  filter(!grepl("test", tolower(external_sub_user_id)))%>%
  group_by(service_id, external_user_id, call_result) %>%
  summarise(qtyByType = sum(n())) %>%
  arrange(service_id, external_user_id) %>%
  group_by(service_id, external_user_id) %>%
  mutate(qtyTotal = sum(qtyByType),
         perQtyType = qtyByType / qtyTotal,
         segment = cut(perQtyType, breaks = c(0, 0.71, 0.80, 0.90, 1), include.lowest = TRUE, labels = c("Poor", "Medium", "Good", "Excellent"))) %>%
  filter(call_result == "ANSWERED") %>%
  group_by(service_id, segment) %>%
  summarise(qtyUsers = sum(n())) %>%
  mutate(perUsers = scales::percent(qtyUsers / sum(qtyUsers))) %>%
  inner_join(dfRawService[, c("service_id", "website_name")], by = c("service_id")) %>%
  group_by() %>%
  select("website_name", "segment", "qtyUsers", "perUsers")