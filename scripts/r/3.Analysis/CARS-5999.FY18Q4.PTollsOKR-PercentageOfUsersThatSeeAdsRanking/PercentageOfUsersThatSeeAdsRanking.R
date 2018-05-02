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


jqlDAURanking <- 
  'function main() {{
return Events({{
from_date: "{startDate}",
to_date:   "{endDate}"
}})
.filter(e=>e.name.includes("my_ads_ranking")===true
&& 
(
  !e.properties.$referring_domain
  || (e.properties.$referring_domain && e.properties.$referring_domain.includes("fixeads")) === false
)
)
.groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets),"distinct_id", "properties.business_status", "properties.platform", "properties.ad_id", "name"],
mixpanel.reducer.count());
}}'

jqlDAURanking <-
  glue(jqlDAURanking)


jqlDAUMyAccount <- 
  '
function main() {{
return Events({{
from_date: "{startDate}",
to_date:   "{endDate}",
event_selectors:[
{{event:"my_account"}},
{{event:"my_ads_active"}},
{{event:"my_ads_pending"}},
{{event:"my_ads_closed"}},
{{event:"my_ads_moderated"}}          ]
}})
.groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "distinct_id", "properties.business_status", "properties.platform", "name"], mixpanel.reducer.count());
}}
'

jqlDAUMyAccount <- 
  glue(jqlDAUMyAccount)

jqlWAURanking <-
  gsub("mixpanel.daily_time_buckets", "mixpanel.weekly_time_buckets", jqlDAURanking)

jqlWAUMyAccount <-
  gsub("mixpanel.daily_time_buckets", "mixpanel.weekly_time_buckets", jqlDAUMyAccount)

# all time ---

jqlRanking <-
  gsub('mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), ', '', jqlDAURanking)

jqlMyAccount <-
  gsub('mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), ', '', jqlDAUMyAccount)


print(paste(Sys.time(), "Looping accounts"))

for(i in listMixpanelAccounts){
  
  print(paste(Sys.time(), "Getting JQL DAU Ranking", i[[1]]))
  
  dfJqlDAURanking <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlDAURanking,
                       columnNames = c("epoch_day", "distinct_id", "business_status", "platform", "ad_id", "event", "quantity"),
                       toNumeric = c(1, 7) 
      )
    )

  print(paste(Sys.time(), "Getting JQL DAU Account", i[[1]]))
  
  
  dfJqlDAUMyAccount <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlDAUMyAccount,
                       columnNames = c("epoch_day", "distinct_id", "business_status", "platform", "event", "quantity"),
                       toNumeric = c(1, 6) 
      )
    )
  
  print(paste(Sys.time(), "Getting JQL WAU Ranking", i[[1]]))
  
  dfJqlWAUMyAccount <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlWAUMyAccount,
                       columnNames = c("epoch_week", "distinct_id", "business_status", "platform", "event", "quantity"),
                       toNumeric = c(1, 6) 
      )
    )
  
  dfJqlMyAccount <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlMyAccount
                       #columnNames = c("distinct_id", "business_status", "platform", "event", "quantity"),
                       #toNumeric = c(1, 6) 
      )
    )
  
  print(paste(Sys.time(), "Getting JQL WAU Account", i[[1]]))
  
  dfJqlWAURanking <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlWAURanking,
                       columnNames = c("epoch_week", "distinct_id", "business_status", "platform", "ad_id", "event", "quantity"),
                       toNumeric = c(1, 7) 
      )
    )

  dfJqlRanking <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlRanking
                       #columnNames = c("distinct_id", "business_status", "platform", "ad_id", "event", "quantity"),
                       #toNumeric = c(6) 
      )
    )
  
  
  # daily values-----------------------------------------------------------------
  print(paste(Sys.time(), "Summarizing Daily Values", i[[1]]))
  
  dfDAURanking <-
    dfJqlDAURanking %>%
    filter(business_status %in% c("business")) %>%
    mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
    select(-epoch_day) %>%
    group_by(day, distinct_id, platform, event) %>%
    summarise(quantity = sum(quantity, na.rm = TRUE)) %>%
    spread(key = event,value = quantity) %>%
    mutate(
      UniqueMyAdsRankingRefresh = !is.na(my_ads_ranking_refresh),
      UniqueMyAdsRankingTooltip = !is.na(my_ads_ranking_tooltip),
      UniqueMyAdsRankingViewInListing = !is.na(my_ads_ranking_view_in_listing)
    ) %>%
    group_by(day, platform) %>%
    summarise(
      totalUniqueMyAdsRankingRefresh = sum(UniqueMyAdsRankingRefresh),
      totalUniqueMyAdsRankingTooltip = sum(UniqueMyAdsRankingTooltip),
      totalUniqueMyAdsRankingViewInListing = sum(UniqueMyAdsRankingViewInListing)
    )
  
  dfDAUinMyAccount <-
    dfJqlDAUMyAccount %>%
    filter(business_status %in% c("business")) %>%
    filter(event %in% 
             c("my_account", "my_ads_active", "my_ads_pending", "my_ads_closed",
               "my_ads_moderated")
    )%>%
    mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
    group_by(day, distinct_id, platform)%>%
    summarise(quantity = sum(quantity))%>%
    group_by(day, platform) %>%
    summarise(users = sum(n())
    )
  
  
  # weekly values-----------------------------------------------------------------
  print(paste(Sys.time(), "Summarizing Weekly Values", i[[1]]))
  
  dfWAURanking <-
    dfJqlWAURanking %>%
    filter(business_status %in% c("business")) %>%
    mutate(week = anytime(as.numeric(epoch_week)/1000)) %>%
    select(-epoch_week) %>%
    group_by(week, distinct_id, platform, event) %>%
    summarise(quantity = sum(quantity, na.rm = TRUE)) %>%
    spread(key = event,value = quantity) %>%
    mutate(
      UniqueMyAdsRankingRefresh = !is.na(my_ads_ranking_refresh),
      UniqueMyAdsRankingTooltip = !is.na(my_ads_ranking_tooltip),
      UniqueMyAdsRankingViewInListing = !is.na(my_ads_ranking_view_in_listing)
    ) %>%
    group_by(week, platform) %>%
    summarise(
      totalUniqueMyAdsRankingRefresh = sum(UniqueMyAdsRankingRefresh),
      totalUniqueMyAdsRankingTooltip = sum(UniqueMyAdsRankingTooltip),
      totalUniqueMyAdsRankingViewInListing = sum(UniqueMyAdsRankingViewInListing)
    )
  
  dfWAUinMyAccount <-
    dfJqlWAUMyAccount %>%
    filter(business_status %in% c("business")) %>%
    filter(event %in% 
             c("my_account", "my_ads_active", "my_ads_pending", "my_ads_closed",
               "my_ads_moderated")
    )%>%
    mutate(week = anytime(as.numeric(epoch_week)/1000)) %>%
    group_by(week, distinct_id, platform)%>%
    summarise(quantity = sum(quantity))%>%
    group_by(week, platform) %>% 
    summarise(users = sum(n())
    )
  
  dfDAU <-
    dfDAUinMyAccount %>%
    inner_join(dfDAURanking, by = c("day", "platform")) %>%
    mutate(CTR = totalUniqueMyAdsRankingRefresh / users)
  
  dfWAU <-
    dfWAUinMyAccount %>%
    inner_join(dfWAURanking, by = c("week", "platform")) %>%
    mutate(CTR = totalUniqueMyAdsRankingRefresh / users)

  
  print(paste(Sys.time(), "Save to AWS", i[[1]]))
  
  s3saveRDS(x = dfDAU,
            object = paste0("dfDAU_", account = i[[1]],".RDS"),
            bucket = "pyrates-data-ocean/CARS-5999")
  
  s3saveRDS(x = dfWAU,
            object = paste0("dfWAU_", account = i[[1]],".RDS"),
              bucket = "pyrates-data-ocean/CARS-5999")
}
