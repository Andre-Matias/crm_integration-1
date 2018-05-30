# OtomotoPL - Experiment ID 10683340850
# Original (paused) (ID: 10678460801)
# Opt-out (ID: 10675851192)
# Otp-in (ID: 10679411919)
# 
# AutovitRO - Experiment ID 10676011185 
# Original (paused) (ID: 10681842136)
# Opt-out (ID: 10677813038)
# Opt-in (ID: 10677772369)
# 
# StandvirtualPT - Experiment ID 10678802031
# Original (paused) (ID: 10677532678)
# Opt-out (ID: 10680901791)
# Opt-in (ID: 10680171521)

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
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, "10683340850"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, "10676011185")
       #standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, "10678802031")
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
  return e.name.indexOf("ab_test_multipay_") > -1 
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
  return [firstEvent.renew_vas, firstEvent.experiments]
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
  
  dfChunk <- gather(dfChunk, "Experiment", "ExperimentID", -1:-3)
  
  dfChunk$Experiment <-  NULL
  
  dfChunk <- dfChunk[grepl(pattern = idExperiment, dfChunk$ExperimentID), ]
  
  dfChunk$project <- as.character(i[[1]])
  
  colnames(dfChunk) <- c("distinct_id", "event", "value", "ExperimentID", "project")

  if(nrow(dfTmp) == 0){
    dfTmp <- dfChunk
  } else {
    dfTmp <- rbind(dfTmp, dfChunk)
  }
}


dfTmp <- spread(data = dfTmp, key = event, value = value)

dfStats <-
  dfTmp %>% 
  group_by(ExperimentID, project, ab_test_multipay_page, ab_test_multipay_finished) %>%
  summarise(qty = sum(n())) %>%
  mutate(per = qty / sum(qty),
         prettyPercentage = scales::percent(per)
         )