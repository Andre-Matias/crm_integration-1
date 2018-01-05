###################################################################################################
# OKR 2 processing                                                                                #
###################################################################################################


# Clean & prepare datasets for OKR2retention analysis ---------------------------------------------


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
