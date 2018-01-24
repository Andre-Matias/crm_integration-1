###################################################################################################
# OKR 1 processing                                                                                #
###################################################################################################


# Load libraries ----------------------------------------------------------------------------------
library(dplyr)
library(tidyr)

# Import required functions -----------------------------------------------------------------------
source("functions.R")


# Clean & prepare datasets for OKR1 retention analysis --------------------------------------------

# PL
df_any_pl <- rbind(df_any_pl_a, df_any_pl_b, df_any_pl_c1, df_any_pl_c2)
ret_any_pl <- prepare_for_retention(df_any_pl, same_day="no")
ret_any_table_pl <- to_wide_table(ret_any_pl)

## compare daily okr1 7 days retention
daily_okr1_pl_a <- daily_okr1 (df_any_pl_a)
daily_okr1_pl_b <- daily_okr1 (df_any_pl_b)
daily_okr1_pl_c1 <- daily_okr1 (df_any_pl_c1)
daily_okr1_pl_c2 <- daily_okr1 (df_any_pl_c2)
comp_okr1_pl <- rbind(daily_okr1_pl_a, daily_okr1_pl_b, daily_okr1_pl_c1, daily_okr1_pl_c2)

## TEST
# comp_okr1_pl <- filter (comp_okr1_pl, platform=="rwd")
#ggplot(data = comp_okr1_pl, mapping=aes(x=dates, y=ret_per)) + geom_line(mapping= aes(colour = version))


# PT
df_any_pt <- rbind(df_any_pt_a, df_any_pt_b, df_any_pt_c1, df_any_pt_c2)
ret_any_pt <- prepare_for_retention(df_any_pt, same_day="no")
ret_any_table_pt <- to_wide_table(ret_any_pt)

## compare daily okr1 7 days retention
daily_okr1_pt_a <- daily_okr1 (df_any_pt_a)
daily_okr1_pt_b <- daily_okr1 (df_any_pt_b)
daily_okr1_pt_c1 <- daily_okr1 (df_any_pt_c1)
daily_okr1_pt_c2 <- daily_okr1 (df_any_pt_c2)
comp_okr1_pt <- rbind(daily_okr1_pt_a, daily_okr1_pt_b, daily_okr1_pt_c1, daily_okr1_pt_c2)

# RO 
df_any_ro <- rbind(df_any_ro_a, df_any_ro_b, df_any_ro_c1, df_any_ro_c2)
ret_any_ro <- prepare_for_retention(df_any_ro, same_day="no")
ret_any_table_ro <- to_wide_table(ret_any_ro)

## compare daily okr1 7 days retention
daily_okr1_ro_a <- daily_okr1 (df_any_ro_a)
daily_okr1_ro_b <- daily_okr1 (df_any_ro_b)
daily_okr1_ro_c1 <- daily_okr1 (df_any_ro_c1)
daily_okr1_ro_c2 <- daily_okr1 (df_any_ro_c2)
comp_okr1_ro <- rbind(daily_okr1_ro_a, daily_okr1_ro_b, daily_okr1_ro_c1, daily_okr1_ro_c2)


# Save it into Amazon S3 --------------------------------------------------------------------------
## Save ret_any_2 PL (with column platform)
## Save ret_any_2 PT (with column platform)
## Save ret_any_2 RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)

