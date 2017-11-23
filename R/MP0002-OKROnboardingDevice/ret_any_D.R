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


# Extract Retention data --------------------------------------------------------------------------
## Will send a query for each account

## Define timerange 
from_date <- "20170904"
to_date <- "20171119"

device_vec <- c("rwd", "desktop", "ios", "android")

## Otomoto PL --------------------
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


## Standvirtual PT ----------------------------------
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

## Autovit RO ----------------------------------
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

## Without platform segmentation? (only use this for old dash, with consolidated countries) -------
## or see if I can sum up each country df


# Clean & prepare datasets for retention analysis -------------------------------------------------
ret_any_pl <- prepare_for_retention(df_any_pl)
ret_any_table_pl <- to_wide_table(ret_any_pl)

ret_any_pt <- prepare_for_retention(df_any_pt)
ret_any_table_pt <- to_wide_table(ret_any_pt)

ret_any_ro <- prepare_for_retention(df_any_ro)
ret_any_table_ro <- to_wide_table(ret_any_ro)


# Save it into Amazon S3 -------------------------------------------------------------------------
## Save ret_any_2 PL (with column platform)
## Save ret_any_2 PT (with column platform)
## Save ret_any_2 RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)
