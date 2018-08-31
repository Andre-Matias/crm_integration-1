################################### INTRO #########################################################
#' Clusters of user behaviour
#' 
#' Metrics used to build clusters:
# -- sessions
# -- searches
# -- ad page views
# -- picture swipe
# -- reply_phone_show
# -- reply_message_sent
# -- reply_phone_call
# -- reply_phone_sms
#' 
#' countries: standvirtual pt
#' scope: desktop & mobile
#' sources: Hydra (via Yamato)
#' period used for analysis: Jan 2018
#' 
#' ################################################################################################


setwd("~/Documents/clusters")

# Load RPostgreSQL library
library("RPostgreSQL")
library("tidyverse")

# Yamato auth -----------------------------------------------------------------
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

# Extract data for clustering --------

requestDB <-
  dbSendQuery(
    conDB,
    "
  select
  session_long,
    count(distinct session) as sessions,
    min(session_long_seq),
    sum(case when trackname IN ('ad_page') then 1 else 0 END) AS ad_page,
    sum(case when trackname IN ('search') then 1 else 0 END) AS searches,
    sum(case when trackname IN ('gallery_swipe') then 1 else 0 END) AS gallery_swipe,
    sum(case when trackname IN ('reply_phone_show') then 1 else 0 END) AS reply_phone_show,
    sum(case when trackname IN ('reply_message_sent') then 1 else 0 END) AS replies,
    sum(case when trackname IN ('reply_phone_call') then 1 else 0 END) AS reply_phone_call,
    sum(case when trackname IN ('reply_phone_sms') then 1 else 0 END) AS reply_phone_sms,
    sum(case when trackname IN ('posting_add_click') then 1 else 0 END) AS posting_add_click
  from hydra_verticals.web
  where 1=1
    and accept_cookies='t'
    and br='standvirtual'
    and server_date_day >= '2018-01-01' and server_date_day <= '2018-01-31'
  group by session_long
  order by sessions desc
;
    ")

users_df <- dbFetch(requestDB)



# A bit of Exploratory Analysis -----------------------------------------------

summary(users_df)

## Number of sessions
ggplot(users_df, mapping = aes(x=num_sessions)) +
  geom_histogram(binwidth = 0.25)
### Most of users have only one sessions, over 70% of total users

## let's zoom on rare bins
ggplot(users_df, mapping = aes(x=num_sessions)) +
  geom_histogram(binwidth = 0.25) +
  coord_cartesian(ylim=c(0, 10000))

## Ad pages
ggplot(users_df, mapping = aes(x=ad_page)) +
  geom_histogram(binwidth = 0.25)

ggplot(users_df, mapping = aes(x=replies)) +
  geom_histogram(binwidth = 0.25)



# Prepare dataset for clustering ----------------------------------------------
df_for_cluster <- users_df %>%
  mutate(leads= reply_phone_show + replies + reply_phone_call + reply_phone_sms) %>%
  select(num_sessions, ad_page, searches, leads, posting_add_click)  



# Run kmeans -----------------------------------------------------------------
km_model <- kmeans(df_for_cluster, centers=5, nstart=1)

## Look into the model
print(km_model)

plot(df_for_cluster, col=km_model$cluster, 
     main="k-means with 5 clusters")



# Try different numbers of clusters ------------------------------------------

## Initialize total within sum of squares error: wss
wss <- 0

## For 1 to 15 cluster centers
for (i in 1:10) {
  km.out <- kmeans(df_for_cluster, centers = i, nstart=1)
  # Save total within sum of squares to wss variable
  wss[i] <- km.out$tot.withinss
}

## Plot total within sum of squares vs. number of clusters: elbow technique
plot(1:10, wss, type = "b", 
     xlab = "Number of Clusters", 
     ylab = "Within groups sum of squares")


