###################################################################################################
# OKR 2 processing                                                                                #
###################################################################################################


# Clean & prepare datasets for OKR2 leads analysis ---------------------------------------------

# PL 
## -- add new versions if needed

df_lead_pl <- rbind(df_lead_pl_a, df_lead_pl_b, df_lead_pl_c1, df_lead_pl_c2)
ret_lead_pl <- prepare_for_retention(df_lead_pl, same_day="yes") # using same function as in OKR1, fixed it!
ret_lead_table_pl <- to_wide_table(ret_lead_pl)

## compare daily okr2 2-14 days retention
daily_okr2_pl_a <- daily_okr2 (df_lead_pl_a)
daily_okr2_pl_b <- daily_okr2 (df_lead_pl_b)
daily_okr2_pl_c1 <- daily_okr2 (df_lead_pl_c1)
daily_okr2_pl_c2 <- daily_okr2 (df_lead_pl_c2)
comp_okr2_pl <- rbind(daily_okr2_pl_a, daily_okr2_pl_b, daily_okr2_pl_c1, daily_okr2_pl_c2)



# PT
df_lead_pt <- rbind(df_lead_pt_a, df_lead_pt_b, df_lead_pt_c1, df_lead_pt_c2)
ret_lead_pt <- prepare_for_retention(df_lead_pt, same_day="yes") 
ret_lead_table_pt <- to_wide_table(ret_lead_pt)

## compare daily okr2 2-14 days retention
daily_okr2_pt_a <- daily_okr2 (df_lead_pt_a)
daily_okr2_pt_b <- daily_okr2 (df_lead_pt_b)
daily_okr2_pt_c1 <- daily_okr2 (df_lead_pt_c1)
daily_okr2_pt_c2 <- daily_okr2 (df_lead_pt_c2)
comp_okr2_pt <- rbind(daily_okr2_pt_a, daily_okr2_pt_b, daily_okr2_pt_c1, daily_okr2_pt_c2)

# RO
df_lead_ro <- rbind(df_lead_ro_a, df_lead_ro_b, df_lead_ro_c1, df_lead_ro_c2)
ret_lead_ro <- prepare_for_retention(df_lead_ro, same_day="yes") 
ret_lead_table_ro <- to_wide_table(ret_lead_ro)

## compare daily okr2 2-14 days retention
daily_okr2_ro_a <- daily_okr2 (df_lead_ro_a)
daily_okr2_ro_b <- daily_okr2 (df_lead_ro_b)
daily_okr2_ro_c1 <- daily_okr2 (df_lead_ro_c1)
daily_okr2_ro_c2 <- daily_okr2 (df_lead_ro_c2)
comp_okr2_ro <- rbind(daily_okr2_ro_a, daily_okr2_ro_b, daily_okr2_ro_c1, daily_okr2_ro_c2)



###################################################################################################
# Save as .RData (both OKR1 and OKR2 dataframes) --------------------------------------------------
last_update <- date()
save(ret_any_pl, ret_any_pt, ret_any_ro, ret_any_table_pl, ret_any_table_pt, ret_any_table_ro,
     ret_lead_pl, ret_lead_pt, ret_lead_ro, ret_lead_table_pl, ret_lead_table_pt, ret_lead_table_ro,
     comp_okr1_pl, comp_okr1_pt, comp_okr1_ro,
     comp_okr2_pl, comp_okr2_pt, comp_okr2_ro,
     last_update,
     file="ret_files.RData")
