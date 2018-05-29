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

dfTmp <- as_tibble()

for(i in listMixpanelAccounts){
  
  dfChunk <- as_tibble()
  
  jqlQuery <- 
    '
    function main() {{
      return Events({{
      from_date: "{startDate}",
      to_date:   "{endDate}"
    }})
      .filter((e) => {{
      return (
      e.name == "my_ads_1_click_vas_modal"
      || e.name == "ab_test_my_ads_1_click_vas_modal_confirm"
      )
      && e.properties.experiments
      && e.properties.experiments.toString().indexOf("{idExperiment}") > -1
      && (
      !e.properties.$referring_domain
      || (e.properties.$referring_domain && e.properties.$referring_domain.includes("fixeads")) === false
      )
      }})
      .groupByUser(["name"],
      (state, events) => {{
      var firstEvent = events.length  ? events[0].properties : null; 
      return [firstEvent. buy_vas_olx]
      }})
      }};
    '
  
  print(paste(Sys.time(), "Sending JQL Query", i[[1]]))
  idExperiment <- as.character(i[[3]])
  
  jqlQuery <- glue(jqlQuery)
  
  dfChunk <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQuery
      )
    )
  
  if(nrow(dfChunk) > 0){
  dfChunk$project <- as.character(i[[1]])
  }
  
  if(nrow(dfTmp) == 0){
    dfTmp <- dfChunk
  } else {
    dfTmp <- rbind(dfTmp, dfChunk)
  }
}

colnames(dfTmp) <- c("distinct_id", "event", "value", "project")


dfStats <-
  dfTmp %>% 
  select("project", "distinct_id", "event", "value") %>%
  spread(key = event, value = value) %>%
  group_by(project, my_ads_1_click_vas_modal, ab_test_my_ads_1_click_vas_modal_confirm) %>%
  summarise(qtyUsers = sum(n())) %>%
  mutate(perUsers = qtyUsers / sum(qtyUsers)
         )
