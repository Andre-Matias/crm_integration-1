# Otomoto Experiment ID 	10681132330 
# Original (ID: 10681730938)
# OLX-VAS (ID: 10682451746)
# 
# Autovit Experiment ID 	10683724453
# Original (ID: 10684413850)
# OLX-VAS (ID: 10681184783)
# 
# Standvirtual Experiment ID 	10677703738
# Original (ID: 10675063749)
# OLX-VAS (ID: 10678944125)

print(paste(Sys.time(), "Loading libraries"))
# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("showtext")
library("glue")
library("aws.s3")

print(paste(Sys.time(), "Loading credentials"))
# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

print(paste(Sys.time(), "Loading list of accounts"))

listMixpanelAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, "10681132330")#,
       #autovitro = list("AutovitRO", mixpanelAutovitAccount, "10683724453")
       #standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, "10677703738")
  )

print(paste(Sys.time(), "Define Start/End Date"))
startDate <- as.character("2018-05-21")
endDate <- as.character(Sys.Date())

dfTmp <- as_tibble()


# for(i in listMixpanelAccounts){
#   
#   dfChunk <- as_tibble()
#   
#   jqlQuery <- 
#     '
#   function main() {{
#   return Events({{
#   from_date: "{startDate}",
#   to_date:   "{endDate}"
#   }})
#   .filter((e) => {{
#   return e.name.indexOf("ab_test_multipay_") > -1 
#   && e.properties.experiments
#   && e.properties.experiments.toString().indexOf("{idExperiment}") > -1
#   && (
#   !e.properties.$referring_domain
#   || (e.properties.$referring_domain && e.properties.$referring_domain.includes("fixeads")) === false
#   )
#   }})
#   .groupByUser(["name"],
#   (state, events) => {{
#   var firstEvent = events.length  ? events[0].properties : null; 
#   return [firstEvent.buy_vas_olx]
#   }})
#   }}
#   '
#   
#   
#   print(paste(Sys.time(), "Sending JQL Query", i[[1]]))
#   idExperiment <- as.character(i[[3]])
#   
#   jqlQuery <- glue(jqlQuery)
#   
#   dfChunk <- 
#     as_tibble(
#       mixpanelJQLQuery(account = i[[2]], 
#                        jqlQuery
#       )
#     )
#   
#   #dfChunk <- gather(dfChunk, "Experiment", "ExperimentID", -1:-3)
#   
#   #dfChunk$Experiment <-  NULL
#   
#   #dfChunk <- dfChunk[grepl(pattern = idExperiment, dfChunk$ExperimentID), ]
#   
#   #dfChunk$project <- as.character(i[[1]])
#   
#   #colnames(dfChunk) <- c("distinct_id", "event", "value", "ExperimentID", "project")
#   
#   if(nrow(dfTmp) == 0){
#     dfTmp <- dfChunk
#   } else {
#     dfTmp <- rbind(dfTmp, dfChunk)
#   }
# }


dfOtomoto <-
  read.csv2("~/Downloads/otomoto.csv", sep = ",")

dfAutovit <-
  read.csv2("~/Downloads/autovit.csv", sep = ",")

dfStandvirtual <-
  read.csv2("~/Downloads/standvirtual.csv", sep = ",")

dfOtomoto$vas <-
  dfOtomoto$value.0.0 %in% c('highlight', 'topads')  |
  dfOtomoto$value.0.1 %in% c('highlight', 'topads' ) |
  dfOtomoto$value.0.2 %in% c('highlight', 'topads' ) |
  dfOtomoto$value.0 %in% c('highlight', 'topads' )

dfOtomoto$value.0 <- NULL
dfOtomoto$value.0.0 <- NULL
dfOtomoto$value.0.1 <- NULL
dfOtomoto$value.0.2 <- NULL

dfAutovit$vas <-
  dfAutovit$value.0.0 %in% c('highlight', 'topads')  |
  dfAutovit$value.0.1 %in% c('highlight', 'topads' ) |
  dfAutovit$value.0 %in% c('highlight', 'topads' )

dfAutovit$value.0 <- NULL
dfAutovit$value.0.0 <- NULL
dfAutovit$value.0.1 <- NULL

dfStandvirtual$vas <-
  dfStandvirtual$value.0.0 %in% c('highlight', 'topads')  |
  dfStandvirtual$value.0.1 %in% c('highlight', 'topads' )

dfStandvirtual$value.0.0 <- NULL
dfStandvirtual$value.0.1 <- NULL

dfOtomoto_Stats <-
  dfOtomoto %>%
  spread(key = key.1, value = vas) %>%
  select(key.0, ab_test_multipay_page, ab_test_multipay_finished) %>%
  group_by(ab_test_multipay_page, ab_test_multipay_finished) %>%
  summarise(qty = sum(n())) %>%
  mutate(per = qty / sum(qty),
         prettyPer = scales::percent(per)
         )

dfAutovit_Stats <-
  dfAutovit %>%
  spread(key = key.1, value = vas) %>%
  select(key.0, ab_test_multipay_page, ab_test_multipay_finished) %>%
  group_by(ab_test_multipay_page, ab_test_multipay_finished) %>%
  summarise(qty = sum(n())) %>%
  mutate(per = qty / sum(qty),
         prettyPer = scales::percent(per)
  )

dfStandvirtual_Stats <-
  dfStandvirtual %>%
  spread(key = key.1, value = vas) %>%
  select(key.0, ab_test_multipay_page, ab_test_multipay_finished) %>%
  group_by(ab_test_multipay_page, ab_test_multipay_finished) %>%
  summarise(qty = sum(n())) %>%
  mutate(per = qty / sum(qty),
         prettyPer = scales::percent(per)
  )

