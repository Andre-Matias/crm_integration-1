# load libraries --------------------------------------------------------------
library("RMixpanel")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

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







