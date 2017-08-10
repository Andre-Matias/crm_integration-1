# set current -----------------------------------------------------------------
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# load libraries --------------------------------------------------------------
library("RMixpanel")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")

source(file = "../../jql/NewUsersCohortTimeToFirstReply.jql")

jqlQuery <- '
function main() {
  return Events({
    from_date: "2017-08-01",
    to_date: "2017-08-01"
  })
  .groupByUser(mixpanel.reducer.count())
}'

result <- mixpanelJQLQuery(mixpanelOtomotoAccount, jqlQuery)





