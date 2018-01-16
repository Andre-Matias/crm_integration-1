###################################################################################################
# OKR 1 processing                                                                                #
###################################################################################################


# Load libraries
library(dplyr)
library(tidyr)

# Import required functions
source("functions.R")


# Clean & prepare datasets for OKR1 retention analysis --------------------------------------------

# PL
ret_any_pl <- prepare_for_retention(df_any_pl)
ret_any_table_pl <- to_wide_table(ret_any_pl)

ret_any_pl_pwa <- prepare_for_retention(df_any_pl_pwa)
ret_any_table_pl_pwa <- to_wide_table(ret_any_pl_pwa)

## compare daily okr1 7 days retention
daily_okr1_pl <- daily_okr1 (df_any_pl)
daily_okr1_pl_pwa <- daily_okr1 (df_any_pl_pwa)
comp_okr1_pl <- left_join(daily_okr1_pl, daily_okr1_pl_pwa, by=c("platform", "dates"))

# PT
ret_any_pt <- prepare_for_retention(df_any_pt)
ret_any_table_pt <- to_wide_table(ret_any_pt)

ret_any_pt_pwa <- prepare_for_retention(df_any_pt_pwa)
ret_any_table_pt_pwa <- to_wide_table(ret_any_pt_pwa)

## compare daily okr1 7 days retention
daily_okr1_pt <- daily_okr1 (df_any_pt)
daily_okr1_pt_pwa <- daily_okr1 (df_any_pt_pwa)
comp_okr1_pt <- left_join(daily_okr1_pt, daily_okr1_pt_pwa, by=c("platform", "dates"))

# RO
ret_any_ro <- prepare_for_retention(df_any_ro)
ret_any_table_ro <- to_wide_table(ret_any_ro)

ret_any_ro_pwa <- prepare_for_retention(df_any_ro_pwa)
ret_any_table_ro_pwa <- to_wide_table(ret_any_ro_pwa)

## compare daily okr1 7 days retention
daily_okr1_ro <- daily_okr1 (df_any_ro)
daily_okr1_ro_pwa <- daily_okr1 (df_any_ro_pwa)
comp_okr1_ro <- left_join(daily_okr1_ro, daily_okr1_ro_pwa, by=c("platform", "dates"))


# Save it into Amazon S3 -------------------------------------------------------------------------
## Save ret_any_2 PL (with column platform)
## Save ret_any_2 PT (with column platform)
## Save ret_any_2 RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)

# keep only weeks from 20 Nov for PWA data
# week_test <- c("2017-11-20", "2017-11-27", "2017-12-04", "2017-12-11", "2017-12-18", "2017-12-25")
# 
# ret_any_pl_pwa <- filter(ret_any_pl_pwa, week %in% week_test)
# ret_any_table_pl_pwa <- filter(ret_any_table_pl_pwa, week %in% week_test)
# 
# ret_any_pt_pwa <- filter(ret_any_pt_pwa, week %in% week_test)
# ret_any_table_pt_pwa <- filter(ret_any_table_pt_pwa, week %in% week_test)
# 
# ret_any_ro_pwa <- filter(ret_any_ro_pwa, week %in% week_test)
# ret_any_table_ro_pwa <- filter(ret_any_table_ro_pwa, week %in% week_test)
