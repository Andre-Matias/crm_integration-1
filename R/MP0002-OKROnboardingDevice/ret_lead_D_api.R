###################################################################################################
# OKR2 retention                                                                                  #
# Also query retention reports through Mixpanel API                                               #
# use the custom event created "make_lead_okr2"                                                   #
###################################################################################################



###################################################################################################
# Extract data from Mixpanel API ------------------------------------------------------------------


# Otomoto PL ------------------------------------------------------------------
## to query "make_lead" custom event use: $custom_event:750077 as per chrome console
df_lead_pl <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
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
  df_lead_pl <- rbind(df_lead_pl, tmp)
}

#### PWA version on A/B test
# started "20171122"
df_lead_pl_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (oto_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            event="$custom_event:750077",
                            from="20171122", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pl_pwa <- rbind(df_lead_pl_pwa, tmp)
}



# Standvirtual PT -------------------------------------------------------------
## to query "make_lead" custom event use: # $custom_event:753501 as per chrome console
df_lead_pt <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
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
  df_lead_pt <- rbind(df_lead_pt, tmp)
}


#### PWA version on A/B test
# started "20171121"
df_lead_pt_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (sta_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            event="$custom_event:753501",
                            from="20171121", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_pt_pwa <- rbind(df_lead_pt_pwa, tmp)
}


# Autovit RO ------------------------------------------------------------------
## to query "make_lead" custom event use: # $custom_event:753505 as per chrome console
df_lead_ro <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
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
  df_lead_ro <- rbind(df_lead_ro, tmp)
}

#### PWA version on A/B test
# started "20171120"
df_lead_ro_pwa <- data.frame()
for (i in seq_along(device_vec)) {
  string_where <- paste0('properties["cl"] == 1 and properties["platform"] == "', device_vec[i] ,'"')
  tmp <- 
    as.data.frame(
      mixpanelGetRetention (aut_auth, 
                            segment_method= "first",
                            retention_type= "birth",
                            born_event="welcome_screen",
                            born_where= string_where,
                            event="$custom_event:753505",
                            from="20171120", to=to_date, unit="day", intervalCount = 15
      )
    )
  
  tmp$platform <- device_vec[i]
  df_lead_ro_pwa <- rbind(df_lead_ro_pwa, tmp)
}


##################################################################################################
# Clean & prepare datasets for retention analysis ------------------------------------------------

# PL---------------------------------------------------------------------------
ret_lead_pl <- prepare_for_retention2(df_lead_pl) # need to use the same function as in OKR1, fix it!!!
ret_lead_table_pl <- to_wide_table(ret_lead_pl)

ret_lead_pl_pwa <- prepare_for_retention2(df_lead_pl_pwa)
ret_lead_table_pl_pwa <- to_wide_table(ret_lead_pl_pwa)

## compare daily okr2 14 days retention
daily_okr2_pl <- daily_okr2 (df_lead_pl)
daily_okr2_pl_pwa <- daily_okr2 (df_lead_pl_pwa)
comp_okr2_pl <- left_join(daily_okr2_pl, daily_okr2_pl_pwa, by=c("platform", "dates"))

# PT---------------------------------------------------------------------------
ret_lead_pt <- prepare_for_retention2(df_lead_pt)
ret_lead_table_pt <- to_wide_table(ret_lead_pt)

ret_lead_pt_pwa <- prepare_for_retention2(df_lead_pt_pwa)
ret_lead_table_pt_pwa <- to_wide_table(ret_lead_pt_pwa)

## compare daily okr2 14 days retention
daily_okr2_pt <- daily_okr2 (df_lead_pt)
daily_okr2_pt_pwa <- daily_okr2 (df_lead_pt_pwa)
comp_okr2_pt <- left_join(daily_okr2_pt, daily_okr2_pt_pwa, by=c("platform", "dates"))

# RO---------------------------------------------------------------------------
ret_lead_ro <- prepare_for_retention2(df_lead_ro)
ret_lead_table_ro <- to_wide_table(ret_lead_ro)

ret_lead_ro_pwa <- prepare_for_retention2(df_lead_ro_pwa)
ret_lead_table_ro_pwa <- to_wide_table(ret_lead_ro_pwa)

## compare daily okr2 14 days retention
daily_okr2_ro <- daily_okr2 (df_lead_ro)
daily_okr2_ro_pwa <- daily_okr2 (df_lead_ro_pwa)
comp_okr2_ro <- left_join(daily_okr2_ro, daily_okr2_ro_pwa, by=c("platform", "dates"))



###################################################################################################
# Save as .RData (both OKR1 and OKR2 dataframes) --------------------------------------------------
last_update <- date()
save(ret_any_pl, ret_any_pt, ret_any_ro, ret_any_table_pl, ret_any_table_pt, ret_any_table_ro,
     ret_lead_pl, ret_lead_pt, ret_lead_ro, ret_lead_table_pl, ret_lead_table_pt, ret_lead_table_ro,
     ret_any_pl_pwa, ret_any_pt_pwa, ret_any_ro_pwa, ret_any_table_pl_pwa, ret_any_table_pt_pwa, ret_any_table_ro_pwa,
     ret_lead_pl_pwa, ret_lead_pt_pwa, ret_lead_ro_pwa, ret_lead_table_pl_pwa, ret_lead_table_pt_pwa, ret_lead_table_ro_pwa,
     comp_okr1_pl, comp_okr1_pt, comp_okr1_ro,
     comp_okr2_pl, comp_okr2_pt, comp_okr2_ro,
     last_update,
     file="ret_files.RData")
