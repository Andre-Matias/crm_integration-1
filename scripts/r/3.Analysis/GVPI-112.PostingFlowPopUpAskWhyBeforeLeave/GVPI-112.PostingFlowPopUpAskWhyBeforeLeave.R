# load libraries --------------------------------------------------------------
library("RMixpanel")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("aws.s3")

# load credentials file -------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)
# load JQL 
source(file = "~/verticals-bi/scripts/jql/PostingFlowLeavingReason.jql")

dfOtomoto <-
  mixpanelJQLQuery(mixpanelOtomotoAccount, jqlQuery)

dfOtomoto$project <- "Otomoto.PL"

dfAutovit<-
  mixpanelJQLQuery(mixpanelAutovitAccount, jqlQuery)

dfAutovit$project <- "Autovit.RO"

dfStandvirtual<-
  mixpanelJQLQuery(mixpanelStandvirtualAccount, jqlQuery)

dfStandvirtual$project <- "Standvirtual.PT"

dfAll <- do.call(rbind, list(dfOtomoto, dfAutovit, dfStandvirtual))

# define columns names --------------------------------------------------------
colnames(dfAll) <- 
  c("date", "event", "reason", "user_status", "platform", "business_status",
    "value", "project")

# clean -----------------------------------------------------------------------

dfAll[dfAll$event == "posting_leaving_reason_show", c("reason")] <- NA

# summarise -------------------------------------------------------------------
dfAll <-
  dfAll %>%
  mutate(date = as.Date(date)) %>%
  group_by(date, event, reason, user_status, platform,
           business_status, project) %>%
  summarise(qty = sum(as.numeric(value)))

# save it to amazon s3 --------------------------------------------------------

s3saveRDS(x = dfAll,
          object = "PostingFlowDropReasons.RDS",
          bucket = "pyrates-data-ocean/GVPI-112")

