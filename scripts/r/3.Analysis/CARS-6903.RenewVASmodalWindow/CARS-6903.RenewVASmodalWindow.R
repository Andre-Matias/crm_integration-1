# OtomotoPL - Experiment ID 10690523672
# Original (ID: 10684616954)
# checked (ID: 10681563892)
# unchecked (ID: 10676606175)
# 
# AutovitRO - Experiment ID 10682695500
# Original (ID: 10684616954)
# checked (ID: 10681563892)
# unchecked (ID: 10676606175)
# 
# StandvirtualPT - Experiment ID 10675872685
# Original (paused) (ID: 10681553185)
# checked (ID: 10670883221)
# unchecked (ID: 10680022219)




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
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, "10690523672"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, "10682695500"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, "10675872685")
  )

print(paste(Sys.time(), "Define Start/End Date"))
startDate <- as.character("2018-03-01")
endDate <- as.character(Sys.Date())

jqlQuery <-
  '
function main() {
return Events({
from_date: "2018-05-16",
to_date:   "2018-05-22"
})

.filter((e) => {
return e.name.indexOf("ab_test_my_ads_1_click_vas") > -1 
&& e.properties.experiments.toString().indexOf("10690523672") > -1
&& (
!e.properties.$referring_domain
|| (e.properties.$referring_domain && e.properties.$referring_domain.includes("fixeads")) === false
)
})
.groupByUser(["name"],
(state, events) => {
var firstEvent = events.length  ? events[0].properties : null; 
return [firstEvent.action_type, firstEvent.experiments, firstEvent.user_id, firstEvent.checked, firstEvent.c]
});
}
'