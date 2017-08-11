# set current -----------------------------------------------------------------
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# load libraries --------------------------------------------------------------
library("RMixpanel")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

# load JQL 
source(file = "../../jql/NewUsersCohortTimeToFirstReply.jql")

result <- mixpanelJQLQuery(mixpanelOtomotoAccount, jqlQuery)





