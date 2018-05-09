# load libraries --------------------------------------------------------------
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("showtext")
library("glue")
library("aws.s3")
library("lubridate")
library("quantmod")

options(scipen = 9999)

load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

dfAll <- as_tibble()

for(i in listAccounts){
  
  querySQl <-
    "
    SELECT id, user_id, params 
    FROM ads 
    WHERE status = 'active' 
    AND category_id = 29
    AND net_ad_counted = 1
    ;
    "

  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(i[[3]]), 
      dbname = i[[4]]
    )
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  dbDisconnect(conDB)
  
  dfSqlQuery$project <- as.character(i[[1]])
  
  if(nrow(dfAll) == 0){
    dfAll <- dfSqlQuery
  } else {
    dfAll <- rbind(dfAll, dfSqlQuery)
  }
}

s3saveRDS(
  x = dfAll, 
  bucket = bucket_path, 
  object = "CARS/CARS-6657/dfAllActiveAds.RDS"
)



t0 <-
  dfAll %>%
  unnest(params = strsplit(params, "<br>")) %>%
  mutate(new = strsplit(params, "<=>"),
         length = lapply(new, length)) %>%
  filter(length >= 2 ) %>%
  mutate(paramName = unlist(lapply(new, function(x) x[1])),
         paramValue = unlist(lapply(new, function(x) x[2]))
  ) %>%
  select(project, id, user_id, paramName, paramValue)

t0[t0$paramName=="price" & !is.na(as.numeric(t0$paramValue)), c("paramName")] <- "priceValue"

t0$paramName <- gsub("price\\[currency\\]", "price_currency", t0$paramName, perl = TRUE)
t0$paramName <- gsub("price\\[gross_net\\]", "price_gross_net", t0$paramName, perl = TRUE)

dfParams <- 
  t0 %>% 
  filter(paramName != "features") %>%
  group_by(project, id, user_id, paramName) %>%
  summarise(paramValue = max(paramValue)) %>%
  filter(paramName %in% c("make", "model", "price_currency", "price_gross_net", "priceValue")) %>%
  spread(key = paramName, value = paramValue)

dfParams$priceValueGross <- 0 
dfParams$priceValue <- as.numeric(dfParams$priceValue)

dfParams$priceValueGross[dfParams$price_gross_net=="net" &dfParams$project == "AutovitRO"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="net" & dfParams$project == "AutovitRO"] * 1.19

dfParams$priceValueGross[dfParams$price_gross_net=="gross" &dfParams$project == "AutovitRO"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="gross" & dfParams$project == "AutovitRO"]

dfParams$priceValueGross[dfParams$price_gross_net=="net" &dfParams$project == "OtomotoPL"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="net" & dfParams$project == "OtomotoPL"] * 1.23

dfParams$priceValueGross[dfParams$price_gross_net=="gross" &dfParams$project == "OtomotoPL"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="gross" & dfParams$project == "OtomotoPL"]

dfParams$priceValueGross[dfParams$price_gross_net=="net" &dfParams$project == "StandvirtualPT"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="net" & dfParams$project == "StandvirtualPT"] * 1.23

dfParams$priceValueGross[dfParams$price_gross_net=="gross" &dfParams$project == "StandvirtualPT"] <- 
  dfParams$priceValue[dfParams$price_gross_net=="gross" & dfParams$project == "StandvirtualPT"]

# PLN to EUR
# 0.233804365
# RON to EUR
# 0.215112863

dfParams$priceValueGross[dfParams$price_currency == "PLN"] <-
  dfParams$priceValueGross[dfParams$price_currency == "PLN"] * 0.233804365

dfParams$priceValueGross[dfParams$price_currency == "RON"] <-
  dfParams$priceValueGross[dfParams$price_currency == "RON"] * 0.215112863


dfParams_AutovitRO <-
  dfParams %>%
  filter(project == "AutovitRO") 

dfParams_AutovitRO$segment <- 
  cut(
    dfParams_AutovitRO$priceValueGross, 
    breaks=c(quantile(dfParams_AutovitRO$priceValueGross, probs = seq(0, 1, by = 0.25))),
    include.lowest=TRUE, dig.lab = 7
    )

dfParams_AutovitRO_Stats <-
  dfParams_AutovitRO %>%
  group_by(project, make, segment) %>%
  summarise(qtyListings = sum(n()),
            qtyUniqueUsers = n_distinct(user_id))


dfParams_OtomotoPL <-
  dfParams %>%
  filter(project == "OtomotoPL") 

dfParams_OtomotoPL$segment <- 
  cut(
    dfParams_OtomotoPL$priceValueGross, 
    breaks=c(quantile(dfParams_OtomotoPL$priceValueGross, probs = seq(0, 1, by = 0.25))),
    include.lowest=TRUE, dig.lab = 7
  )

dfParams_OtomotoPL_Stats <-
  dfParams_OtomotoPL %>%
  group_by(project, make, segment) %>%
  summarise(qtyListings = sum(n()),
            qtyUniqueUsers = n_distinct(user_id))


dfParams_StandvirtualPT <-
  dfParams %>%
  filter(project == "StandvirtualPT") 

dfParams_StandvirtualPT$segment <- 
  cut(
    dfParams_StandvirtualPT$priceValueGross, 
    breaks=c(quantile(dfParams_StandvirtualPT$priceValueGross, probs = seq(0, 1, by = 0.25))),
    include.lowest=TRUE, dig.lab = 7
  )

dfParams_StandvirtualPT_Stats <-
  dfParams_StandvirtualPT %>%
  group_by(project, make, segment) %>%
  summarise(qtyListings = sum(n()),
            qtyUniqueUsers = n_distinct(user_id))

