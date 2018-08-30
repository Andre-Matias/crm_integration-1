#######################################################################################################################
#' Analysis: Users by Package type
#' 
#' verticals: otomoto, autovit, standvirtual
#' 
#' how package works:
#' - otomoto/autovit: post-paid, only one package at time
#' - standvirtual: pre-paid, users can have more than package at time --> site assigns the ad to the highest package
#' 
#'
#'#####################################################################################################################


setwd("~/Verticals-bi/scripts/r/3.Analysis/CARS-7912.PackagesCarParts/")

# Load libraries
library("config")
library("DBI")
library("RMySQL")
library("dplyr")
library("tidyr")

# Load personal credentials
load("~/Documents/r_scripts_miei/personal_credentials.Rdata")


# Extract from Otomoto PL db replica ==========================================

## load db credential
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "otomoto_pl") )
conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= OtomotoPsw,
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname = config$DbName
)

# SQL query
sql_pl <- "
    SELECT
      month(created_at_first) as  month_of_ad_creation,
      package_id,
      name,
      count(distinct usuario) as user_count
    FROM
    (
      SELECT ad_id, A.user_id as usuario, category_id, created_at_first, package_id, starting_time, ending_time, description
      FROM
      (SELECT
      id as ad_id,
      user_id,
      category_id,
      created_at_first,
      description
      FROM otomotopl.ads
      WHERE
      created_at_first >= '2018-01-01 00:00:00' AND created_at_first < '2018-07-30 00:00:00'
      AND category_id = 163

      AND net_ad_counted = 1
      ) A
      INNER JOIN otomotopl.billing_periods BP
      ON A.user_id = BP.user_id
      AND A.created_at_first >= BP.starting_time
      AND A.created_at_first < BP.ending_time
    ) Z
      LEFT JOIN
      (SELECT id, name FROM otomotopl.dealer_packages) DP
      ON Z.package_id = DP.id
      
      
      GROUP BY month_of_ad_creation, package_id, name
  ;"


# Extract data
packages_pl <-dbGetQuery(conDB, sql_pl)

write_csv(packages_pl, "packages_month_pl.csv")

#close connection
dbDisconnect(conDB)






# Extract from Autovit RO db replica ==============================================================

config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "autovit_ro") )

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= AutovitPsw,
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname = config$DbName
)

# SQL query
sql_ro <- "
    SELECT
      month(created_at_first) as  month_of_ad_creation,
      package_id,
      name,
      count(distinct usuario) as user_count
    FROM
    (
      SELECT ad_id, A.user_id as usuario, category_id, created_at_first, package_id, starting_time, ending_time, description
      FROM
      (SELECT
      id as ad_id,
      user_id,
      category_id,
      created_at_first,
      description
      FROM autovitro.ads
      WHERE
      created_at_first >= '2018-01-01 00:00:00' AND created_at_first < '2018-07-30 00:00:00'
      AND category_id = 69
      
      AND net_ad_counted = 1
      ) A
      INNER JOIN autovitro.billing_periods BP
      ON A.user_id = BP.user_id
      AND A.created_at_first >= BP.starting_time
      AND A.created_at_first < BP.ending_time
    ) Z
      LEFT JOIN
      (SELECT id, name FROM autovitro.dealer_packages) DP
      ON Z.package_id = DP.id
      
      
      GROUP BY month_of_ad_creation, package_id, name
  ;"
  
  
  # Extract data
  packages_ro <-dbGetQuery(conDB, sql_ro)

  
  #close connection
  dbDisconnect(conDB)

  
  write_csv(packages_ro, "packages_month_ro.csv")

  
  
  
  
# Standvirtual ------------------------------------------------------
  
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                        config = Sys.getenv("R_CONFIG_ACTIVE", "standvirtual_pt") )
  
conDB<- dbConnect(MySQL(), 
                    user= config$DbUser, 
                    password= StandvirtualPsw,
                    host= config$DbHost, 
                    port= config$DbPort,
                    dbname = config$DbName
  )
  

# SQL query
  sql_pt <- "
  SELECT
    *
  FROM
  (
  SELECT ad_id, A.user_id as usuario, category_id, created_at_first, package_id, starting_time, ending_time, description
  FROM
  (SELECT
  id as ad_id,
  user_id,
  category_id,
  created_at_first,
  description
  FROM carspt.ads
  WHERE
  created_at_first >= '2018-01-01 00:00:00' AND created_at_first < '2018-07-30 00:00:00'
  AND category_id = 661
  
  AND net_ad_counted = 1
  ) A
  INNER JOIN carspt.ondemand_packages OP
  ON A.user_id = OP.user_id
  AND A.created_at_first >= OP.starting_time
  AND A.created_at_first < OP.ending_time
  )Z
  LEFT JOIN
  (SELECT id, name, display_order FROM carspt.dealer_packages) DP
  ON Z.package_id = DP.id
  ;"  
  
  
# Extract data
packages_pt <-dbGetQuery(conDB, sql_pt)
  

#close connection
dbDisconnect(conDB)


# Process dataset -------------------------------------------------------------
packages_pt$description <- NULL

# check duplicated ads
duplicated<- packages_pt %>%
  group_by(ad_id) %>%
  count() %>%
  arrange(desc(n)) %>%
  head()
## example: 8008104630 had 3 rows
filter(packages_pt, ad_id=="8008104630")
rm(duplicated)

# Filter only top packages when there is more than one package attributed to the same ad
only_top_package <- packages_pt %>%
  group_by(ad_id) %>%
  arrange(desc(display_order)) %>%
  filter(row_number()==1)

only_top_package$month <- format(as.Date(only_top_package$created_at_first), "%Y-%m")

# Summarize user count by month and package
packages_month_pt <- only_top_package %>%
  #select(month, package_id, name, usuario, ad_id) %>%
  group_by(month, package_id, name) %>%
  summarize(user_count = n_distinct(usuario)) %>%
  rename(month_of_ad_creation = month)
  
  
write_csv(packages_month_pt, "packages_month_pt.csv")
  




# ================================================================================================
# Query ads table via Yamato ----------------------------------------------------------------------
# Load libraries
library("RPostgreSQL")
library("tidyverse")
library("stringr")


# Yamato auth -----
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "yamato") )

drv <- dbDriver("PostgreSQL")
conDB <-
  dbConnect(
    drv,
    user= YamatoUser, 
    password= YamatoPsw,
    host= config$DbHost, 
    port= config$DbPort,
    dbname = config$DbName
  )

# Extract ads data --------

requestDB <-
  dbSendQuery(
    conDB,
    "
    select 
      id,
      user_id,
      created_at_first
    FROM livesync.verticals_ads
    where livesync_dbname='carspt'
    and category_id=661
    and created_at_first >= '2018-01-01 00:00:00' AND created_at_first < '2018-07-30 00:00:00'
    AND net_ad_counted = 1
    ;
    
    "
  )


ads_data <- dbFetch(requestDB)  # around 8M rows


# Extract from ondemand_packages --------------
conDB<- dbConnect(MySQL(), 
                  user= "bi_team_pt", 
                  password= "SpYtKStpSoBzybD3mrJNRFjgf2yj3H",
                  host= "10.29.0.140", 
                  port= 3308,
                  dbname = "carspt"
)


# SQL query
sql_ondemand <- "
select *
from carspt.ondemand_packages
;"  
  
  
# Extract ondemand
ondemand_data <-dbGetQuery(conDB, sql_ondemand)

# SQL query
sql_dealer_packages <- "
select *
from carspt.dealer_packages
;"  


# Extract data
dp_data <-dbGetQuery(conDB, sql_dealer_packages)


# assign each transaction to a package
df_transations_with_packages <- 
  ads_data %>%
  left_join(df_dealer_packages, by = c("user_id")) %>%
  filter(created_at_first >= starting_time, created_at_first <= ending_time) %>%
  group_by(id) %>%
  arrange(desc(display_order)) %>%
  filter(row_number()==1)

  
# assign each transaction to a package
df_transations_with_packages <- 
  df_transactions %>%
  left_join(df_dealer_packages, by = c("user_id")) %>%
  filter(datetime >= starting_time, datetime <= ending_time) %>%
  group_by(id) %>%
  arrange(desc(display_order)) %>%
  filter(row_number()==1)