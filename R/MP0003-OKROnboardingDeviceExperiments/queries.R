
#######################################################################################################################
# Queries to extract data for OKR1, OKR2 from Mixpanel                                                                #                                      #                                                                               #
#######################################################################################################################


# Set working directory
## setwd("~/verticals-bi/R/MP0003-OKROnboardingDeviceExperiments")

# Load mixpanel library ---------------------------------------------------------------------------
library(RMixpanel)
library(dplyr)


# MixPanel account authentication -----------------------------------------------------------------
## Fill in here the API token, key and secret as found on 
## www.mixpanel.com - Account -> Projects. 

## Otomoto PL
oto_auth = mixpanelCreateAccount("otomoto.pl",
                                 token="b2b9c69bb88736c7e833e9d609004e6a",
                                 secret="ed06fd545816f0ef5c79f4936e603870", key="" 
)
## Standvirtual PT
sta_auth <- mixpanelCreateAccount("standvirtual.pt",
                                  token="b605c76ba537a8c09202d3b04c5acfa5",
                                  secret="4b51dbcc5f322d07f0ac40ea490813b8", key="" 
)
## Autovit RO
aut_auth <- mixpanelCreateAccount("autovit.ro",
                                  token="adfe0536b9eb9cc099f7f35a4c7c9a02",
                                  secret="79e6da126de6456734165fa9fc1dc98a", key="" 
)



## Define timerange: valid across countries -------------------------------------------------------
from_date <- "20171127"
to_date <- "20180318"
## Define platforms to loop through in case of original version (no experiment id assigned)
device_vec <- c("rwd", "desktop", "ios", "android")

## Create a lookup table for experiments e.g.: ----------------------------------------------------
# experiment id, country, platform, start_date, finish_date, description
exp_lookup <- tribble(
  ~experiment_id, ~version, ~country,  ~platform, ~start_date, ~description,
  "na",           "b",      "all",     "rwd",     "20171120", "1st iteration starting with welcome_screen",
  "46817:1241071","c1",     "pl",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions",
  "46817:1241061","c2",     "pl",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions but removing segments",
  "47658:1269381","c1",     "pt",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions",
  "47658:1269391","c2",     "pt",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions but removing segments",
  "47656:1269291","c1",     "ro",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions",
  "47656:1269301","c2",     "ro",      "ios",     "20180125", "2nd iteration starting with welcome_screen with UX optimizazions but removing segments"
)



# OKR1 queries ====================================================================================
#' will send a query for each account                                                                                 



## Otomoto PL -----------------------------------------------------------------

# original - home for all platforms
df_any_pl_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pl_a <- rbind(df_any_pl_a, tmp)
}

df_any_pl_a <- mutate(df_any_pl_a, version="original A")


# Variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_any_pl_b <- as.data.frame(
                  mixpanelGetRetention (oto_auth, 
                          segment_method= "first",
                          retention_type= "birth",
                          born_event="welcome_screen",
                          born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                          from="20171120", to=to_date, unit="day", intervalCount = 15
                  )
               )

df_any_pl_b <- mutate(df_any_pl_b, platform="rwd", version="variation B")

  
# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started ""
# df_any_pl_b1 <- as.data.frame(
#                    mixpanelGetRetention(oto_auth, 
#                           segment_method= "first", 
#                           retention_type= "birth", 
#                           born_event="welcome_screen", 
#                           born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
#                           and (string(properties["platform"]) == "ios")', 
#                           from=20171204, to=20180116, unit="day", intervalCount = 15 
#                    )
#                 )
# df_any_pl_b1 <- mutate(df_any_pl_b1, platform="ios", version="variation B1")
df_any_pl_b1 <- data.frame()


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions but removing segments
## experiments: "ios_onboarding_c1"
## started ""
# df_any_pl_c1 <- as.data.frame(
#   mixpanelGetRetention(oto_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# 
# df_any_pl_c1 <- mutate(df_any_pl_c1, platform="ios", version="variation C1") 
df_any_pl_c1 <- data.frame()




## Standvirtual PT --------------------------------------------------------------------------------
# original - home for all platforms
df_any_pt_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (sta_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pt_a <- rbind(df_any_pt_a, tmp)
}

df_any_pt_a <- mutate(df_any_pt_a, version="original A")

# variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_any_pt_b <- as.data.frame(
  mixpanelGetRetention (sta_auth, 
                        segment_method= "first",
                        retention_type= "birth",
                        born_event="welcome_screen",
                        born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                        from="20171120", to=to_date, unit="day", intervalCount = 15
  )
)

df_any_pt_b <- mutate(df_any_pt_b, platform="rwd", version="variation B")


# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started "20180313"
df_any_pt_b1 <- as.data.frame(
  mixpanelGetRetention(sta_auth, 
                       segment_method= "first", 
                       retention_type= "birth", 
                       born_event="welcome_screen", 
                       born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
                       and (string(properties["platform"]) == "ios")', 
                       from=20180313, to=to_date, unit="day", intervalCount = 15 
  )
)
df_any_pt_b1 <- mutate(df_any_pt_b1, platform="ios", version="variation B1")


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions but removing segments
## experiments: "ios_onboarding_c1"
## started ""
df_any_pt_c1 <- as.data.frame(
  mixpanelGetRetention(sta_auth, 
                       segment_method= "first", 
                       retention_type= "birth", 
                       born_event="welcome_screen", 
                       born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
                       and (string(properties["platform"]) == "ios")', 
                       from=20180313, to=to_date, unit="day", intervalCount = 15 
  )
)

df_any_pt_c1 <- mutate(df_any_pt_c1, platform="ios", version="variation C1") 


## Autovit RO -------------------------------------------------------------------------------------
# original - home for all platforms
df_any_ro_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (aut_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_ro_a <- rbind(df_any_ro_a, tmp)
}

df_any_ro_a <- mutate(df_any_ro_a, version="original A")

# variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_any_ro_b <- as.data.frame(
  mixpanelGetRetention (aut_auth, 
                        segment_method= "first",
                        retention_type= "birth",
                        born_event="welcome_screen",
                        born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                        from="20171120", to=to_date, unit="day", intervalCount = 15
  )
)

df_any_ro_b <- mutate(df_any_ro_b, platform="rwd", version="variation B")


# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started ""
# df_any_ro_b1 <- as.data.frame(
#   mixpanelGetRetention(aut_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# df_any_ro_b1 <- mutate(df_any_ro_b1, platform="ios", version="variation B1")
df_any_ro_b1 <- data.frame()


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizations but removing segments
## experiments: "ios_onboarding_c1"
## started ""
# df_any_ro_c1 <- as.data.frame(
#   mixpanelGetRetention(aut_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# 
# df_any_ro_c1 <- mutate(df_any_ro_c1, platform="ios", version="variation C1") 
df_any_ro_c1 <- data.frame()





# OKR2 queries=====================================================================================                                                                                            #
#' will send a query for each account                                                                                                                   
#' use the custom event created "make_lead_okr2"                                                                      



# Otomoto PL ------------------------------------------------------------------
# original - home for all platforms
## to query "make_lead" custom event use: $custom_event:750077 as per chrome console
df_lead_pl_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            event="$custom_event:750077",
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pl_a <- rbind(df_lead_pl_a, tmp)
}

df_lead_pl_a <- mutate(df_lead_pl_a, version="original A")


# Variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_lead_pl_b <- as.data.frame(
  mixpanelGetRetention (oto_auth, 
                        segment_method= "first",
                        retention_type= "birth",
                        born_event="welcome_screen",
                        born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                        event="$custom_event:750077",
                        from="20171120", to=to_date, unit="day", intervalCount = 15
  )
)

df_lead_pl_b <- mutate(df_lead_pl_b, platform="rwd", version="variation B")

# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started ""
# df_lead_pl_b1 <- as.data.frame(
#   mixpanelGetRetention(oto_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        event="$custom_event:750077",
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# df_lead_pl_b1 <- mutate(df_lead_pl_b1, platform="ios", version="variation B1")
df_lead_pl_b1 <- data.frame()


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions but removing segments
## experiments: "ios_onboarding_c1"
## started ""
# df_lead_pl_c1 <- as.data.frame(
#   mixpanelGetRetention(oto_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        event="$custom_event:750077",
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# 
# df_lead_pl_c1 <- mutate(df_lead_pl_c1, platform="ios", version="variation C1") 
df_lead_pl_c1 <- data.frame()



# Standvirtual PT -------------------------------------------------------------
# original - home for all platforms
## to query "make_lead" custom event use: # $custom_event:753501 as per chrome console
df_lead_pt_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (sta_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            event="$custom_event:753501",
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pt_a <- rbind(df_lead_pt_a, tmp)
}


df_lead_pt_a <- mutate(df_lead_pt_a, version="original A")

# Variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_lead_pt_b <- as.data.frame(
  mixpanelGetRetention (sta_auth, 
                        segment_method= "first",
                        retention_type= "birth",
                        born_event="welcome_screen",
                        born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                        event="$custom_event:753501",
                        from="20171120", to=to_date, unit="day", intervalCount = 15
  )
)

df_lead_pt_b <- mutate(df_lead_pt_b, platform="rwd", version="variation B")

# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started "20180313"
df_lead_pt_b1 <- as.data.frame(
  mixpanelGetRetention(sta_auth, 
                       segment_method= "first", 
                       retention_type= "birth", 
                       born_event="welcome_screen", 
                       born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
                       and (string(properties["platform"]) == "ios")', 
                       event="$custom_event:753501",
                       from=20180313, to=to_date, unit="day", intervalCount = 15 
  )
)
df_lead_pt_b1 <- mutate(df_lead_pt_b1, platform="ios", version="variation B1")


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions but removing segments
## experiments: "ios_onboarding_c1"
## started "20180313"
df_lead_pt_c1 <- as.data.frame(
  mixpanelGetRetention(sta_auth, 
                       segment_method= "first", 
                       retention_type= "birth", 
                       born_event="welcome_screen", 
                       born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
                       and (string(properties["platform"]) == "ios")', 
                       event="$custom_event:753501",
                       from=20180313, to=to_date, unit="day", intervalCount = 15 
  )
)

df_lead_pt_c1 <- mutate(df_lead_pt_c1, platform="ios", version="variation C1") 



# Autovit RO ------------------------------------------------------------------
# original - home for all platforms
## to query "make_lead" custom event use: # $custom_event:753505 as per chrome console
df_lead_ro_a <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('number(properties["cl"]) == 1 and (string(properties["platform"]) == "', device_vec[i] ,'")')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (aut_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            event="$custom_event:753505",
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_ro_a <- rbind(df_lead_ro_a, tmp)
}

df_lead_ro_a <- mutate(df_lead_ro_a, version="original A")


# Variation B - rwd
## 1st onboarding version starting from welcome_screen
## started "20171122"

df_lead_ro_b <- as.data.frame(
  mixpanelGetRetention (aut_auth, 
                        segment_method= "first",
                        retention_type= "birth",
                        born_event="welcome_screen",
                        born_where= 'number(properties["cl"]) == 1 and (string(properties["platform"]) == "rwd")',
                        event="$custom_event:753505",
                        from="20171120", to=to_date, unit="day", intervalCount = 15
  )
)

df_lead_ro_b <- mutate(df_lead_ro_b, platform="rwd", version="variation B")

# variation B1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions
## experiments: "ios_onboarding_b1"
## started ""
# df_lead_ro_b1 <- as.data.frame(
#   mixpanelGetRetention(aut_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_b1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        event="$custom_event:753505",
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# df_lead_ro_b1 <- mutate(df_lead_ro_b1, platform="ios", version="variation B1")
df_lead_ro_b1 <- data.frame()


# variation C1 - ios
## 2nd onboarding version starting with welcome_screen with UX optimizazions but removing segments
## experiments: "ios_onboarding_c1"
## started ""
# df_lead_ro_c1 <- as.data.frame(
#   mixpanelGetRetention(aut_auth, 
#                        segment_method= "first", 
#                        retention_type= "birth", 
#                        born_event="welcome_screen", 
#                        born_where= '"ios_onboarding_c1" in (properties["experiments"]) 
#                        and (string(properties["platform"]) == "ios")', 
#                        event="$custom_event:753505",
#                        from=20171204, to=20180116, unit="day", intervalCount = 15 
#   )
# )
# 
# df_lead_ro_c1 <- mutate(df_lead_ro_c1, platform="ios", version="variation C1") 
df_lead_ro_c1 <- data.frame()

