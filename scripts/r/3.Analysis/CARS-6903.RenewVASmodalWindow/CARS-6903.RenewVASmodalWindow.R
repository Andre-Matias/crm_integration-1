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
        return e.name.indexOf("ab_test_my_ads_1_click_vas") > -1 
        && e.properties.experiments.toString().indexOf("{idExperiment}") > -1
        && (
          !e.properties.$referring_domain
          || (e.properties.$referring_domain && e.properties.$referring_domain.includes("fixeads")) === false
        )
      }})
      .groupByUser(["name"],
                   (state, events) => {{
                     var firstEvent = events.length  ? events[0].properties : null; 
                     return [firstEvent.action_type, firstEvent.user_id, firstEvent.checked, firstEvent.c, firstEvent.experiments]
                   }});
  }}
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
  
  dfChunk <- gather(dfChunk, "Experiment", "ExperimentID", -1:-6)
  
  dfChunk$project <- as.character(i[[1]])
  
  dfChunk$Experiment <-  NULL
  dfChunk$V2 <-  NULL
  dfChunk$V4 <-  NULL
  dfChunk$V6 <-  NULL
  
  colnames(dfChunk) <- c("distinct_id", "event", "value", "ExperimentID", "project")
  
  dfChunk <- dfChunk[grepl(pattern = idExperiment, dfChunk$ExperimentID), ]
  
  if(nrow(dfTmp) == 0){
    dfTmp <- dfChunk
  } else {
    dfTmp <- rbind(dfTmp, dfChunk)
  }
}

dfTmp <- spread(data = dfTmp, key = event, value = value)

dfStats <-
  dfTmp %>% 
  group_by(ExperimentID, project, ab_test_my_ads_1_click_VAS_modal, ab_test_my_ads_1_click_VAS_modal_confirm) %>%
  summarise(qty = sum(n())) %>%
  mutate(per = qty / sum(qty))
