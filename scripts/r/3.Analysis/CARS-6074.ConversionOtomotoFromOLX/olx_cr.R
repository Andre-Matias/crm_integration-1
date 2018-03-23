#' Otomoto leads coming from OLX
#' 
#' Background: changes have been made on OLX ad pages that redirect to Otomoto on 13/02/2018 
#' Check if conversion rate of users coming from OLX has changed
#' look at trend daily/weekly since January 2018
#' 
#' scope: users coming from OLX
#' platforms: desktop/mobile
#' ##################################################################




# Load RPostgreSQL library ------------------------------------------------
library("RPostgreSQL")
library("tidyverse")



drv <- dbDriver("PostgreSQL")

# Connect to Yamato database ----------------------------------------------
conDB <-
  dbConnect(
    drv,
    host = "10.101.5.237",
    port = 5671,
    dbname = "main",
    user = "marco_pasin",
    password = "your psw"
  )

# Sessions from OLX -------------------------------------------------
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      server_date_day,
      count(distinct session),
      case
        WHEN platform_type IS NOT NULL THEN platform_type
        ELSE CASE WHEN user_agent like '%Mobi%' then 'mobile-html5'
          else 'desktop' end
      END platform
    from hydra_verticals.web
    where 1=1
      and br='otomoto'
      and server_date_day >= '2018-01-01'
      and referer like '%olx%'
    group by server_date_day, platform
    order by server_date_day
    ;
    "
  )

sessions <- dbFetch(requestDB)



# Leads from OLX -------------------------------------------------
requestDB <-
  dbSendQuery(
    conDB,
    "
    select
      server_date_day,
      count(trackname),
      case
        WHEN platform_type IS NOT NULL THEN platform_type
        ELSE CASE WHEN user_agent like '%Mobi%' then 'mobile-html5'
          ELSE 'desktop' end
      END as platform
    from hydra_verticals.web
    where 1=1
      and br='otomoto'
      and server_date_day >= '2018-01-01'
      and referer like '%olx%'
      and trackname in ('reply_phone_show','reply_phone_call','reply_phone_sms','reply_message_sent')
    group by server_date_day, platform
    order by server_date_day
    ;
    "
  )

leads <- dbFetch(requestDB)



# Join/clean/export-----------------------------------------------------------------
## will use Tableau for visualization
df <- sessions %>%
  inner_join(leads, by=c("server_date_day", "platform")) %>%
  arrange(server_date_day) %>%
  rename(sessions=count.x, leads=count.y) %>%
  select(server_date_day, platform, sessions, leads) %>%
  mutate(cr= leads/sessions) %>%
  write_csv("leads_from_olx.csv")




