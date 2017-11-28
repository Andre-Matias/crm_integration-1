# Calculate lead retention
#
# Load libraries
library("rjson")
library("data.table")
library("dplyr")
library("dtplyr")
library("stringr")
library("zoo")
library("ggplot2")
library("ggthemes")
library('scales')
library("gridExtra")
library("tidyr")

# Make sure to be in working directory
setwd("~/verticals-bi/R/MP0002-OKROnboardingDevice")

# Import JSON with data from Mixpanel JQL query
json_pl <- fromJSON(file = "./data/retention_lead_platform_pl.json")
json_pt <- fromJSON(file = "./data/retention_lead_platform_pt.json")
json_ro <- fromJSON(file = "./data/retention_lead_platform_ro.json")

# Import JSON for PWA test
json_pl_pwa <- fromJSON(file = "./data/retention_lead_platform_pl_pwa.json")
json_pt_pwa <- fromJSON(file = "./data/retention_lead_platform_pt_pwa.json")
json_ro_pwa <- fromJSON(file = "./data/retention_lead_platform_ro_pwa.json")

# Clean and convert JSON to a tabular format
df_lead_pl <- cleaning_json(json_pl)
df_lead_pt <- cleaning_json(json_pt)
df_lead_ro <- cleaning_json(json_ro)

df_lead_pl_pwa <- cleaning_json(json_pl_pwa)
df_lead_pt_pwa <- cleaning_json(json_pt_pwa)
df_lead_ro_pwa <- cleaning_json(json_ro_pwa)

# Prepare dataset for retention analysis              
ret_lead_pl <- prepare_for_retention_lead(df_lead_pl)
ret_lead_table_pl <- to_wide_table(ret_lead_pl)

ret_lead_pt <- prepare_for_retention_lead(df_lead_pt)
ret_lead_table_pt <- to_wide_table(ret_lead_pt)

ret_lead_ro <- prepare_for_retention_lead(df_lead_ro)
ret_lead_table_ro <- to_wide_table(ret_lead_ro)

##PWA
ret_lead_pl_pwa <- prepare_for_retention_lead(df_lead_pl_pwa)
ret_lead_table_pl_pwa <- to_wide_table(ret_lead_pl_pwa)

ret_lead_pt_pwa <- prepare_for_retention_lead(df_lead_pt_pwa)
ret_lead_table_pt_pwa <- to_wide_table(ret_lead_pt_pwa)

ret_lead_ro_pwa <- prepare_for_retention_lead(df_lead_ro_pwa)
ret_lead_table_ro_pwa <- to_wide_table(ret_lead_ro_pwa)



# Save it into Amazon S3 -------------------------------------------------------------------------
## Save ret_lead_pl PL (with column platform)
## Save ret_lead_pt PT (with column platform)
## Save ret_lead_ro RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)



# for now save as .RData
save(ret_any_pl, ret_any_pt, ret_any_ro, ret_any_table_pl, ret_any_table_pt, ret_any_table_ro,
     ret_lead_pl, ret_lead_pt, ret_lead_ro, ret_lead_table_pl, ret_lead_table_pt, ret_lead_table_ro,
     ret_any_pl_pwa, ret_any_pt_pwa, ret_any_ro_pwa, ret_any_table_pl_pwa, ret_any_table_pt_pwa, ret_any_table_ro_pwa,
     ret_lead_pl_pwa, ret_lead_pt_pwa, ret_lead_ro_pwa, ret_lead_table_pl_pwa, ret_lead_table_pt_pwa, ret_lead_table_ro_pwa,
     file="ret_files.RData")
