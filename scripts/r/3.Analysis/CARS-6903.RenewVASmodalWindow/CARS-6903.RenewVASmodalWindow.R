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

print(paste(Sys.time(), "Loading list of acocunts"))

listMixpanelAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount),
       autovitro = list("AutovitRO", mixpanelAutovitAccount),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount)
  )

print(paste(Sys.time(), "Define Start/End Date"))
startDate <- as.character("2018-03-01")
endDate <- as.character(Sys.Date())