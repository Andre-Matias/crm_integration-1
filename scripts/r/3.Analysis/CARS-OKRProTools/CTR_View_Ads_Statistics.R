#load libraries
library("aws.s3")
library("feather")
library("dplyr")
library("data.table")
library("dtplyr")
library("fasttime")
library("magrittr")
library("ggplot2")
library("RMixpanel")
library("tidyr")
library("scales")
options(scipen = 9999)


# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3BucketName <- 
  "s3://pyrates-data-ocean/"

account = mixpanelCreateAccount(" standvirtual.pt ",
                                token="b605c76ba537a8c09202d3b04c5acfa5",
                                secret="4b51dbcc5f322d07f0ac40ea490813b8", 
                                key="553c55")


jqlQuery <- 
'
function main(){
  return Events({
    from_date: \'2018-01-29\',
    to_date:   \'2018-02-04\',
    event_selectors: [
    {event: \'my_account\'},
    {event: \'my_ads_statistics\'}
    ]
  })
.groupByUser(["name", "properties.platform"], mixpanel.reducer.count())
.groupBy(["key.1", "key.2"], mixpanel.reducer.count())
}
'

dfSTV <- 
  mixpanelJQLQuery(account, jqlQuery, columnNames=c("EventName", "Platform", "UniqueClicks"))

# OTOMOTO ----------
account = mixpanelCreateAccount("otomoto.pl",
                                token="  b2b9c69bb88736c7e833e9d609004e6a",
                                secret="ed06fd545816f0ef5c79f4936e603870", 
                                key="553c55")

jqlQuery <- 
  '
function main(){
return Events({
from_date: \'2018-01-29\',
to_date:   \'2018-02-04\',
event_selectors: [
{event: \'my_account\'},
{event: \'my_ads_statistics\'}
]
})
.groupByUser(["name", "properties.platform"], mixpanel.reducer.count())
.groupBy(["key.1", "key.2"], mixpanel.reducer.count())
}
'

dfOTO <- 
  mixpanelJQLQuery(account, jqlQuery, columnNames=c("EventName", "Platform", "UniqueClicks"))

# AUTOVIT ----------
account = mixpanelCreateAccount("autovit.ro",
                                token="adfe0536b9eb9cc099f7f35a4c7c9a02",
                                secret="79e6da126de6456734165fa9fc1dc98a", 
                                key="553c55")

jqlQuery <- 
  '
function main(){
return Events({
from_date: \'2018-01-29\',
to_date:   \'2018-02-04\',
event_selectors: [
{event: \'my_account\'},
{event: \'my_ads_statistics\'}
]
})
.groupByUser(["name", "properties.platform"], mixpanel.reducer.count())
.groupBy(["key.1", "key.2"], mixpanel.reducer.count())
}
'

dfATV <- 
  mixpanelJQLQuery(account, jqlQuery, columnNames=c("EventName", "Platform", "UniqueClicks"))

# ------

dfSTV <-
  dfSTV %>%
  spread(key = EventName, value = UniqueClicks) %>%
  filter(!is.na(Platform)) %>%
  mutate(CTR = as.numeric(my_ads_statistics) / as.numeric(my_account))

dfATV <-
  dfATV %>%
  spread(key = EventName, value = UniqueClicks) %>%
  filter(!is.na(Platform)) %>%
  mutate(CTR = as.numeric(my_ads_statistics) / as.numeric(my_account))

dfOTO <-
  dfOTO %>%
  spread(key = EventName, value = UniqueClicks) %>%
  filter(!is.na(Platform)) %>%
  mutate(CTR = as.numeric(my_ads_statistics) / as.numeric(my_account))

ggplot(dfOTO)+
  geom_bar(stat="identity", aes(x=Platform, y=CTR, fill=Platform))+
  scale_y_continuous(labels= percent)+geom_text(aes(x=Platform, y=CTR, label=percent(CTR)), vjust=-0.5)+
  ggtitle("CTR - Click View Ads Statistics", subtitle = "Otomoto - 2018-01-29 to 2018-02-04")

ggplot(dfATV)+
  geom_bar(stat="identity", aes(x=Platform, y=CTR, fill=Platform))+
  scale_y_continuous(labels= percent)+geom_text(aes(x=Platform, y=CTR, label=percent(CTR)), vjust=-0.5)+
  ggtitle("CTR - Click View Ads Statistics", subtitle = "Autovit - 2018-01-29 to 2018-02-04")

ggplot(dfSTV)+
  geom_bar(stat="identity", aes(x=Platform, y=CTR, fill=Platform))+
  scale_y_continuous(labels= percent)+geom_text(aes(x=Platform, y=CTR, label=percent(CTR)), vjust=-0.5)+
  ggtitle("CTR - Click View Ads Statistics", subtitle = "Standvirtual - 2018-01-29 to 2018-02-04")

