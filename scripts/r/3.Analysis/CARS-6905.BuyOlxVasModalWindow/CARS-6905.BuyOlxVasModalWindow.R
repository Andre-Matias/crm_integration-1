# Otomoto Experiment ID 	10680473891
# Original (ID: 10693100212)
# Variation #1 (ID: 10677843892)
# 
# Autovit Experiment ID 	10678587448 
# Original (ID: 10684616469)
# Variation #1 (ID: 10690060574)
# 
# Standvirtual Experiment ID 	10682743623 
# Original (ID: 10676043240)
# Variation #1 (ID: 10675063532)

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
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, "10680473891"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, "10678587448"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, "10682743623")
  )

print(paste(Sys.time(), "Define Start/End Date"))
startDate <- as.character("2018-05-21")
endDate <- as.character(Sys.Date())