
#####################################################################################
# Test users who clicked on search in homepage vs users who finished the onboarding #
#                                                                                   #
#####################################################################################

# Otomoto PL ------------------------------------------------------------------

# Retrieve data from Mixpanel as per normal script: 
oto_auth = mixpanelCreateAccount("otomoto.pl",
                                 token="b2b9c69bb88736c7e833e9d609004e6a",
                                 secret="ed06fd545816f0ef5c79f4936e603870", key="" 
)

## Define timerange: valid across countries
from_date <- "20171127"
to_date <- "20171217"
## Define platforms to loop through 
device_vec <- c("rwd", "desktop", "ios", "android")


##########################
# OKR1 
##########################

## users that clicked on search from home, came back and did anything
df_any_pl <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["touch_point_page"] == "home" and properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="search",
                            born_where= string_where,
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pl <- rbind(df_any_pl, tmp)
}

#### users that finished PWA, came back and did anything
# started "20171122"
df_any_pl_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="see_results",
                            born_where= string_where,
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pl_pwa <- rbind(df_any_pl_pwa, tmp)
}


#### users that clicked on "skip_click" button, came back and did anything
# started "20171122"
df_any_pl_pwa_skip <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="skip_click",
                            born_where= string_where,
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pl_pwa_skip <- rbind(df_any_pl_pwa_skip, tmp)
}


##########################
# OKR2 
##########################

## users that clicked on search from home, came back and made a lead
## to query "make_lead" custom event use: $custom_event:750077 as per chrome console
df_lead_pl <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["touch_point_page"] == "home" and properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="search",
                            born_where= string_where,
                            event="$custom_event:750077",
                            from= from_date, to= to_date, unit= "day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pl <- rbind(df_lead_pl, tmp)
}


#### users that finished PWA, came back and made a lead
#### to query "make_lead" custom event use: $custom_event:750077 as per chrome console
df_lead_pl_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="see_results",
                            born_where= string_where,
                            event="$custom_event:750077",
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pl_pwa <- rbind(df_lead_pl_pwa, tmp)
}


#### users that clicked on "skip_click" button, came back and made a lead
df_lead_pl_pwa_skip <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="skip_click",
                            born_where= string_where,
                            event="$custom_event:750077",
                            from=from_date, to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pl_pwa_skip <- rbind(df_lead_pl_pwa_skip, tmp)
}






