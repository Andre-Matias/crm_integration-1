library(RMixpanel)
library(dplyr)
library(tidyr)
#
# MixPanel account authentication --------------------------------------------------------------------
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


#' Extract Retention data --------------------------------------------------------------------------
#' Will send a query for each account

## Define timerange 
from_date <- "20171002"
to_date <- "20171126"
## Define platforms to loop through 
device_vec <- c("rwd", "desktop", "ios", "android")

## Otomoto PL -------------------------------------------------------------------------------------
df_any_pl <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
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
  df_any_pl <- rbind(df_any_pl, tmp)
}

#### PWA version on A/B test
# started "20171122"
df_any_pl_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            from="20171106", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pl_pwa <- rbind(df_any_pl_pwa, tmp)
}


## Standvirtual PT --------------------------------------------------------------------------------
df_any_pt <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (sta_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            from=from_date, to=to_date, unit="day", intervalCount = 15
                           )
                )
  
  tmp$platform <- device_vec[i]
  df_any_pt <- rbind(df_any_pt, tmp)
}


#### PWA version on A/B test
# started "20171121"
df_any_pt_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (sta_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            from="20171106", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_pt_pwa <- rbind(df_any_pt_pwa, tmp)
}


## Autovit RO -------------------------------------------------------------------------------------
df_any_ro <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (aut_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="home",
                            born_where= string_where,
                            from=from_date, to=to_date, unit="day", intervalCount = 15
                           )
                )
  
  tmp$platform <- device_vec[i]
  df_any_ro <- rbind(df_any_ro, tmp)
}

#### PWA version on A/B test
# started "20171120"
df_any_ro_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (aut_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            from="20171106", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_any_ro_pwa <- rbind(df_any_ro_pwa, tmp)
}


## Without platform segmentation? (only use this for old dash, with consolidated countries) -------
## or see if I can sum up each country df


# Clean & prepare datasets for retention analysis -------------------------------------------------

# PL
ret_any_pl <- prepare_for_retention(df_any_pl)
ret_any_table_pl <- to_wide_table(ret_any_pl)

ret_any_pl_pwa <- prepare_for_retention(df_any_pl_pwa)
ret_any_table_pl_pwa <- to_wide_table(ret_any_pl_pwa)

# PT
ret_any_pt <- prepare_for_retention(df_any_pt)
ret_any_table_pt <- to_wide_table(ret_any_pt)

ret_any_pt_pwa <- prepare_for_retention(df_any_pt_pwa)
ret_any_table_pt_pwa <- to_wide_table(ret_any_pt_pwa)

# RO
ret_any_ro <- prepare_for_retention(df_any_ro)
ret_any_table_ro <- to_wide_table(ret_any_ro)

ret_any_ro_pwa <- prepare_for_retention(df_any_ro_pwa)
ret_any_table_ro_pwa <- to_wide_table(ret_any_ro_pwa)


# Save it into Amazon S3 -------------------------------------------------------------------------
## Save ret_any_2 PL (with column platform)
## Save ret_any_2 PT (with column platform)
## Save ret_any_2 RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)

# keep only week from 20 Nov for PWA data
ret_any_pl_pwa <- filter(ret_any_pl_pwa, week=="2017-11-20")
ret_any_table_pl_pwa <- filter(ret_any_table_pl_pwa, week=="2017-11-20")

ret_any_pt_pwa <- filter(ret_any_pt_pwa, week=="2017-11-20")
ret_any_table_pt_pwa <- filter(ret_any_table_pt_pwa, week=="2017-11-20")

ret_any_ro_pwa <- filter(ret_any_ro_pwa, week=="2017-11-20")
ret_any_table_ro_pwa <- filter(ret_any_table_ro_pwa, week=="2017-11-20")
