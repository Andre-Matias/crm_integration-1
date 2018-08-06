################################### INTRO #########################################################
#' We should be able to understand which queries are the main drivers for "no results" listing page.
#' What filters combination are mostly contributing to that?
#' 
#' countries: Otomoto, Standvirtual, Autovit
#' sources: Hydra (via Yamato)
#' period used for analysis: 9-15 July 2018
#' 
#' ################################################################################################

setwd("~/Documents/CARS-7352.NoResultsFilters")

# Load libraries
library("RPostgreSQL")
library("tidyverse")
library("stringr")


# Yamato auth -----------------------------------------------------------------
drv <- dbDriver("PostgreSQL")

# Connect to Yamato database 
conDB <-
  dbConnect(
    drv,
    host = "10.101.5.237",
    port = 5671,
    dbname = "main",
    user = "marco_pasin",
    password = "xxx"
  )


# Extract Yamato data ---------------------------------------------------------

## all searches that returned zero results, along with correspondent filters 
## also grabbing the keyword if used
## filters are contained inside "extra"field
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      br,
      keyword,
      extra
    from hydra_verticals.web
    where 1=1
      and accept_cookies='t'
      and server_date_day >= '2018-07-09' and server_date_day <= '2018-07-15'
      and br IN ('otomoto', 'standvirtual', 'autovit')
      and result_count = 0
      and trackname='search'
    ;
    "
  )

no_results_data <- dbFetch(requestDB)  #around 165k rows

## calculate number of searches with no results per country
## 125k are from Otomoto
no_results_searches <- no_results_data %>%
  group_by(br) %>%
  count()


## also calculate total number of searches per country
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      br,
      count(*)
    from hydra_verticals.web
    where 1=1
      and accept_cookies='t'
      and server_date_day >= '2018-07-09' and server_date_day <= '2018-07-15'
      and br IN ('otomoto', 'standvirtual', 'autovit')
      and trackname='search'
    group by br
    ;
    "
  )

total_searches <- dbFetch(requestDB)  
head(total_searches)
## otomoto: 4.1M
## standvirtual: 777k
## autovit: 558k


# Calculate % of no results searches over total searches 
df_total <- inner_join(total_searches, no_results_searches, by="br") %>%
  rename(total_searches = count, no_results_searches = n) %>%
  write_csv("df_total.csv")
head(df_total)



# Extract filters from the extra field ----------------------------------------
filters <- 
  c("brand", "model", "from_year", "item_condition", "equipment", "to_mileage",
    "from_mileage",  "to_year", "fuel_type", "from_cm3", "to_cm3",
    "damage", "from_price", "to_price", "city_name", "distance_filt", "subregion_id",
    "region_name", "subregion_name", "color", "section", "only_private", "cat_l2_name",
    "only_pros", "metallic", "district_name", "particle_filter", "brand_program", 
    "authorized_dealer", "invs", "invc", "body_type"
  )



## At the moment we have only brand, model, from_price, to_price, item_condition, distance_filt: 
no_results_data$brand <- ifelse(grepl("brand", no_results_data$extra)==TRUE, "brand", "") 
no_results_data$model <- ifelse(grepl("model", no_results_data$extra)==TRUE, "model", "") 


no_results_data$from_price <- ifelse(grepl("from_price", no_results_data$extra)==TRUE, "from_price", "")
no_results_data$to_price <- ifelse(grepl("to_price", no_results_data$extra)==TRUE, "to_price", "")

no_results_data$distance_filt <-  ifelse(grepl("distance_filt", no_results_data$extra)==TRUE, "distance_filt", "")

no_results_data$item_condition_all <- ifelse(grepl("item_condition\":\"all", no_results_data$extra)==TRUE, "item_condition_all", "")
no_results_data$item_condition_new <- ifelse(grepl("item_condition\":\"new", no_results_data$extra)==TRUE, "item_condition_new", "")
no_results_data$item_condition_used <- ifelse(grepl("item_condition\":\"used", no_results_data$extra)==TRUE, "item_condition_used", "")
#no_results_data$item_condition <- grepl("item_condition", no_results_data$extra)

no_results_data$free_text <- ifelse(is.na(no_results_data$keyword)==FALSE, "free_text","")



# Concatenate all filters together --------------------------------------------
no_results_data$filters <- 
  paste(no_results_data$brand, no_results_data$model, no_results_data$from_price, no_results_data$to_price,
        no_results_data$distance_filt, no_results_data$item_condition_all, no_results_data$item_condition_new, 
        no_results_data$item_condition_used, no_results_data$free_text, sep=" ")



# Summarize number of searches by filter combinations -------------------------
## will then visualize it via Tableau
df_viz <- no_results_data %>%
  group_by(br, filters) %>%
  count() %>%
  arrange(desc(n)) %>%
  rename(searches=n) %>%
  write_csv("df_viz.csv")

head(df_viz)


# Next step: possibly analyse the % of searches with no results for each combination of filters.


###################
# Try with all searches if I can find power, fuel, year parameters et.
# requestDB <-
#   dbSendQuery(
#     conDB,
#     "
#     select
#     br,
#     keyword,
#     extra
#     from hydra_verticals.web
#     where 1=1
#     and accept_cookies='t'
#     and server_date_day >= '2018-07-09' and server_date_day <= '2018-07-15'
#     and br IN ('otomoto', 'standvirtual', 'autovit')
#     
#     and trackname='search'
# limit 10000
#     ;
#     "
#   )
# 
# all_results_data <- dbFetch(requestDB)  # around 160k rows (125k from Otomoto)
