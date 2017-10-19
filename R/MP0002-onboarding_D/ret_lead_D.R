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

# Import JSON with data from Mixpanel JQL query
json_pl <- fromJSON(file = "./data/retention_lead_platform_pl.json")
json_pt <- fromJSON(file = "./data/retention_lead_platform_pt.json")
json_ro <- fromJSON(file = "./data/retention_lead_platform_ro.json")

# Clean and convert JSON to a tabular format
df_lead_pl <- cleaning_json(json_pl)
df_lead_pt <- cleaning_json(json_pt)
df_lead_ro <- cleaning_json(json_ro)

# Prepare dataset for retention analysis              
ret_lead_pl <- prepare_for_retention_lead(df_lead_pl)
ret_lead_table_pl <- to_wide_table(ret_lead_pl)

ret_lead_pt <- prepare_for_retention_lead(df_lead_pt)
ret_lead_table_pt <- to_wide_table(ret_lead_pt)

ret_lead_ro <- prepare_for_retention_lead(df_lead_ro)
ret_lead_table_ro <- to_wide_table(ret_lead_ro)


# Save it into Amazon S3 -------------------------------------------------------------------------
## Save ret_lead_pl PL (with column platform)
## Save ret_lead_pt PT (with column platform)
## Save ret_lead_ro RO (with column platform)
## Save ret_any_2 consolidated (no country neither platform)

retPlot(ret_lead_pl, title="lead...", subtitle="cdcecdv")
