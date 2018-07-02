################################### INTRO #########################################################
#' Prior to defining a baseline for Trusted Advisor OKRs, we need to understand if GA data is 
#' reliable.Hence we will make a comparison, especially on conversions, of GA (not validated yet) 
#' vs Hydra (validated already).
#' 
#' metrics: sessions, show_phone, reply_click (TBD for each data source)
#' period: current month (1-25) June 2018
#' countries: Otomoto, Standvirtual, Autovit
#' data sources: GA (via API), Hydra (via Yamato)
#' 
#' 
#' ################################################################################################

setwd("~/Verticals-bi/scripts/r/3.Analysis/CARS-7458.GAvsYamato")

# Load libraries
library("RGoogleAnalytics")
library("RPostgreSQL")
library("tidyverse")


# GA auth ---------------------------------------------------------------------

# client_id <- "xxxx"
# client_secret<- "xxxx"
# client_username <- "marco.pasin@olx.com"
# 
# token <- Auth(client_id,client_secret)
# save(token,file="./token_file")

# Load the token (already available in working directory)
load("./token_file")

# Validate the token
ValidateToken(token)

# List view ids
viewID<-GetProfiles(token)
viewID

oto_id <- "ga:5485250"
sta_id <- "ga:536318"
aut_id <- "ga:22130385"



# Extract data from GA --------------------------------------------------------

start_date <- "2018-06-01"
end_date <- "2018-06-30"


# Otomoto

## reply_click
# query_init <- Init(start.date = start_date,
#                    end.date = end_date,
#                    dimensions = c("ga:date"),
#                    metrics = c( "ga:totalEvents"),
#                    max.results = 10000, 
#                    sort = "ga:date",
#                    # category=ad page & action = contact & ....
#                    filters=c("ga:eventCategory==Ad%20Page; ga:eventAction=@Contact"),
#                    table.id = oto_id)
# 
# query_pl_pho <- QueryBuilder(query_init)
# query_pl_pho <- GetReportData(query_pl_pho, token, paginate_query = F)

## Goals:
### goal6: show_phone
### goal 7: reply_click
query_init <- Init(start.date = start_date,
                   end.date = end_date,
                   dimensions = c("ga:date"),
                   metrics = c( "ga:sessions","ga:goal6Completions", "ga:goal7Completions"),
                   max.results = 10000, 
                   sort = "ga:date",
                   table.id = oto_id)

query_pl_ga <- QueryBuilder(query_init)
query_pl_ga <- GetReportData(query_pl_ga, token, paginate_query = F)

query_pl_ga <- query_pl_ga %>% rename(show_phone= goal6Completions, reply_click= goal7Completions)



# Standvirtual 

## Goals:
### goal14: show_phone
### goal 1: reply_click
query_init <- Init(start.date = start_date,
                   end.date = end_date,
                   dimensions = c("ga:date"),
                   metrics = c( "ga:sessions","ga:goal14Completions", "ga:goal1Completions"),
                   max.results = 10000, 
                   sort = "ga:date",
                   table.id = sta_id)

query_pt_ga <- QueryBuilder(query_init)
query_pt_ga <- GetReportData(query_pt_ga, token, paginate_query = F)

query_pt_ga <- query_pt_ga %>% rename(show_phone= goal14Completions, reply_click= goal1Completions)



# Autovit

## Goals:
### goal18: show_phone desktop
### goal2: show_phone mobile
### reply?? don't know how they are tracked
query_init <- Init(start.date = start_date,
                   end.date = end_date,
                   dimensions = c("ga:date"),
                   metrics = c( "ga:sessions","ga:goal18Completions", "ga:goal2Completions"),
                   max.results = 10000, 
                   sort = "ga:date",
                   table.id = aut_id)

query_ro_ga <- QueryBuilder(query_init)
query_ro_ga <- GetReportData(query_ro_ga, token, paginate_query = F)

query_ro_ga <- query_ro_ga %>% 
  mutate(show_phone= goal18Completions + goal2Completions)




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
    password = "xxxx"
  )


# Extract Yamato data ---------------------------------------------------------


# Otomoto

## sessions
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      server_date_day,
      count(distinct session_long) as sessions
    from hydra_verticals.web
    where 1=1
      and br='otomoto'
      and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_pl_ya_ses <- dbFetch(requestDB)


## prepare tmp table with only sessions with conversions (to be comparable with GA goals)
# select
# server_date_day,
# session_long,
# sum(case when trackname IN ('reply_phone_show') then 1 else 0 END) AS phone_show,
# sum(case when trackname IN ('reply_message_form_click') then 1 else 0 END) AS reply_form,
# sum(case when trackname IN ('reply_message_click') then 1 else 0 END) AS reply_click,
# sum(case when trackname IN ('reply_message_sent') then 1 else 0 END) AS reply_sent
# into sessions_activity_pl
# from hydra_verticals.web
# where 1=1
# -- and accept_cookies='t'
# and br='otomoto'
# and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
# group by server_date_day, session_long
# order by server_date_day
# ;


## phone_show
requestDB <-
  dbSendQuery(
    conDB,
    "
  select
    server_date_day,
    count(session_long) as sessions_w_show_phone
  from marco_pasin.sessions_activity_pl
  where 1=1
  and phone_show >=1
  group by server_date_day
  order by server_date_day
    ;
    "
  )

query_pl_ya_pho <- dbFetch(requestDB)


## reply_click
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      server_date_day,
      count(session_long) as sessions_w_reply
    from marco_pasin.sessions_activity_pl
    where 1=1
      and reply_click >=1
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_pl_ya_rep <- dbFetch(requestDB)

## join
query_pl_ya <- inner_join(query_pl_ya_ses, query_pl_ya_pho, by="server_date_day")
query_pl_ya <- inner_join(query_pl_ya, query_pl_ya_rep, by="server_date_day")


# Standvirtual

## sessions
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(distinct session_long) as sessions
    from hydra_verticals.web
    where 1=1
    and br='standvirtual'
    and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_pt_ya_ses <- dbFetch(requestDB)


## prepare tmp table with only sessions with conversions (to be comparable with GA goals)
# select
# server_date_day,
# session_long,
# sum(case when trackname IN ('reply_phone_show') then 1 else 0 END) AS phone_show,
# sum(case when trackname IN ('reply_message_form_click') then 1 else 0 END) AS reply_form,
# sum(case when trackname IN ('reply_message_click') then 1 else 0 END) AS reply_click,
# sum(case when trackname IN ('reply_message_sent') then 1 else 0 END) AS reply_sent
# into sessions_activity_pt
# from hydra_verticals.web
# where 1=1
# and br='standvirtual'
# and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
# group by server_date_day, session_long
# order by server_date_day
# ;


## phone_show
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(session_long) as sessions_w_show_phone
    from marco_pasin.sessions_activity_pt
    where 1=1
    and phone_show >=1
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_pt_ya_pho <- dbFetch(requestDB)


## reply_click
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(session_long) as sessions_w_reply
    from marco_pasin.sessions_activity_pt
    where 1=1
    and reply_click >=1
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_pt_ya_rep <- dbFetch(requestDB)


## join
query_pt_ya <- inner_join(query_pt_ya_ses, query_pt_ya_pho, by="server_date_day")
query_pt_ya <- inner_join(query_pt_ya, query_pt_ya_rep, by="server_date_day")


# Autovit

## sessions
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(distinct session_long) as sessions
    from hydra_verticals.web
    where 1=1
    and br='autovit'
    and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_ro_ya_ses <- dbFetch(requestDB)


## prepare tmp table with only sessions with conversions (to be comparable with GA goals)
# select
# server_date_day,
# session_long,
# sum(case when trackname IN ('reply_phone_show') then 1 else 0 END) AS phone_show,
# sum(case when trackname IN ('reply_message_form_click') then 1 else 0 END) AS reply_form,
# sum(case when trackname IN ('reply_message_click') then 1 else 0 END) AS reply_click,
# sum(case when trackname IN ('reply_message_sent') then 1 else 0 END) AS reply_sent
# into sessions_activity_ro
# from hydra_verticals.web
# where 1=1
# and br='standvirtual'
# and server_date_day >= '2018-06-01' AND server_date_day <= '2018-06-30'
# group by server_date_day, session_long
# order by server_date_day
# ;


## phone_show
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(session_long) as sessions_w_show_phone
    from marco_pasin.sessions_activity_ro
    where 1=1
    and phone_show >=1
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_ro_ya_pho <- dbFetch(requestDB)


## reply_click
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
    server_date_day,
    count(session_long) as sessions_w_reply
    from marco_pasin.sessions_activity_ro
    where 1=1
    and reply_click >=1
    group by server_date_day
    order by server_date_day
    ;
    "
  )

query_ro_ya_rep <- dbFetch(requestDB)

## join
query_ro_ya <- inner_join(query_ro_ya_ses, query_ro_ya_pho, by="server_date_day")
query_ro_ya <- inner_join(query_ro_ya, query_ro_ya_rep, by="server_date_day")



# Join dataframes, one per country ------------------------------------------------------
df_pl <- bind_cols(query_pl_ga, query_pl_ya) %>%
  rename(sessions_ga = sessions, show_phone_ga = show_phone, reply_click_ga=reply_click,
         sessions_yamato = sessions1, show_phone_yamato = sessions_w_show_phone, reply_click_yamato = sessions_w_reply ) %>%
  mutate(vertical = "otomoto")

df_pt <- bind_cols(query_pt_ga, query_pt_ya) %>%
  rename(sessions_ga = sessions, show_phone_ga = show_phone, reply_click_ga=reply_click,
         sessions_yamato = sessions1, show_phone_yamato = sessions_w_show_phone, reply_click_yamato = sessions_w_reply ) %>%
  mutate(vertical = "standvirtual")

df_ro <- bind_cols(query_ro_ga, query_ro_ya) %>%
  rename(sessions_ga = sessions, show_phone_ga = show_phone,
         sessions_yamato = sessions1, show_phone_yamato = sessions_w_show_phone, reply_click_yamato = sessions_w_reply) %>%
  mutate(vertical = "autovit")

df_comparison <- bind_rows(df_pl, df_pt, df_ro)


# Save locally (will use Tableau for viz)

# write_csv(df_pl, "df_pl.csv")
# write_csv(df_pt, "df_pt.csv")
# write_csv(df_ro, "df_ro.csv")

write_csv(df_comparison, "df_comparison.csv")

