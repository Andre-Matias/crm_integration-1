# libraries -------------------------------------------------------------------
library("RMySQL")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("ggthemes")
library("anytime")
options(scipen = 9999)


# config ---------------------------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

querySQl <-
  "
SELECT YEAR(PUP.date)Year, WEEKOFYEAR(PUP.date)WeekOfYear, DATE(PUP.date)Day, PI.type, COUNT(*)Qty,  SUM(-price)Amount
FROM paidads_user_payments PUP
INNER JOIN paidads_indexes PI
ON PUP.id_index=PI.id
INNER JOIN
payment_session PS
ON PUP.id_transaction=PS.id
WHERE
PUP.date >= '2018-01-01 00:00:00'
AND PS.status = 'finished'
AND PI.type IN('topads', 'highlight', 'ad_homepage', 'export_olx', 'bump_up', 'ad_bighomepage')
AND PUP.is_removed_from_invoice = 0
AND price < 0
AND PS.provider NOT IN('admin')
GROUP BY 1, 2, 3, 4;
"

dbs <-
  list(
    c("OtomotoPL", 3317, "otomotopl"),
    c("AutovitRO", 3315, "autovitro"),
    c("StandvirtualPT", 3308, "carspt")
  )


for(vertical in dbs){
  print(vertical[1])
  print(vertical[2])
  print(vertical[3])
  
  # connect to database  ------------------------------------------------------
  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = as.numeric(vertical[2]), 
      dbname = vertical[3]
    )
  
  dfSqlQuery <-
    dbGetQuery(conDB, querySQl)
  
  assign(paste0("dfQueryResults_", vertical[1]), value = dfSqlQuery)
  
  dbDisconnect(conDB)
}


# exchange rate 31/Jan/2018
# OTO - PLN > USD > 0.29485
# ATV - RON > USD > 0.26702
# STV - EUR > USD > 1.24137

dfQueryResults_OtomotoPL$Amount_USD <- 
  dfQueryResults_OtomotoPL$Amount * 0.29485

dfQueryResults_AutovitRO$Amount_USD <- 
  dfQueryResults_AutovitRO$Amount * 0.26702

dfQueryResults_StandvirtualPT$Amount_USD <- 
  dfQueryResults_StandvirtualPT$Amount * 1.24137

# base line - average 4 weeks january

baseline_OtomotoPL <-
  dfQueryResults_OtomotoPL %>% 
  group_by(WeekOfYear) %>% 
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty)) %>% 
  filter(WeekOfYear >= 10, WeekOfYear <= 13) %>% 
  group_by() %>% 
  summarise(baselineUSD=mean(totalAmount_USD), baselineQTY = mean(totalQTY) ) %>%
  select(baselineUSD, baselineQTY) %>%
  mutate(goalAmount = baselineUSD * 1.10,
         goalQty = baselineQTY * 1.10)

baseline_AutovitRO <-
  dfQueryResults_AutovitRO %>% 
  group_by(WeekOfYear) %>% 
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty)) %>% 
  filter(WeekOfYear >= 10, WeekOfYear <= 13) %>% 
  group_by() %>% 
  summarise(baselineUSD=mean(totalAmount_USD), baselineQTY = mean(totalQTY) ) %>%
  select(baselineUSD, baselineQTY) %>%
  mutate(goalAmount = baselineUSD * 1.10,
         goalQty = baselineQTY * 1.10)

baseline_StandvirtualPT <-
  dfQueryResults_StandvirtualPT %>% 
  group_by(WeekOfYear) %>% 
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty)) %>% 
  filter(WeekOfYear >= 10, WeekOfYear <= 13) %>% 
  group_by() %>% 
  summarise(baselineUSD=mean(totalAmount_USD), baselineQTY = mean(totalQTY) ) %>%
  select(baselineUSD, baselineQTY) %>%
  mutate(goalAmount = baselineUSD * 1.10,
         goalQty = baselineQTY * 1.10)

dfTotal <- 
  rbind(rbind(dfQueryResults_StandvirtualPT, dfQueryResults_AutovitRO), dfQueryResults_OtomotoPL)

baseline_dfTotal <-
  dfTotal %>% 
  group_by(WeekOfYear) %>% 
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty)) %>% 
  filter(WeekOfYear >= 10, WeekOfYear <= 13) %>% 
  group_by() %>% 
  summarise(baselineUSD=mean(totalAmount_USD), baselineQTY = mean(totalQTY) ) %>%
  select(baselineUSD, baselineQTY) %>%
  mutate(goalAmount = baselineUSD * 1.10,
         goalQty = baselineQTY * 1.10)

dfOTOByWeek <- 
  dfQueryResults_OtomotoPL %>%
  group_by(WeekOfYear) %>%
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty))

dfATVByWeek <- 
  dfQueryResults_AutovitRO %>%
  group_by(WeekOfYear) %>%
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty))

dfSTVByWeek <- 
  dfQueryResults_StandvirtualPT %>%
  group_by(WeekOfYear) %>%
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty))

dfTotalByWeek <- 
  dfTotal %>%
  group_by(WeekOfYear) %>%
  summarise(totalAmount_USD = sum(Amount_USD), totalQTY = sum(Qty))

plotit <- 
  function(actuals, goals, platform, actual, metric, goal, kr, ylabel){
    g <- 
      ggplot(actuals)+
      annotate("text", x = 0, y = as.numeric(goals[1,c(metric)]), label = paste("baseline:", round(goals[1,c(metric)],0)), vjust = 1.5, hjust = 0, color='coral')+
      annotate("text", x = 17, y = as.numeric(goals[1,c(goal)]), label = paste("goal:", round(goals[1,c(goal)],0)), vjust = -1.0, hjust = 0.75, color='coral')+
      geom_point(aes(WeekOfYear, y = actuals[, c(actual)]))+
      scale_x_continuous(breaks = seq(1,32,1))+
      scale_y_continuous(limits = c(0, max(actuals[, c(actual)])*1.15))+
      geom_hline(yintercept=as.numeric(goals[1,c(metric)]), color='coral', size=1)+
      geom_hline(yintercept=as.numeric(goals[1,c(goal)]), color='coral', size=1)+
      ylab(ylabel)+
      xlab("weeks")+
      theme_solarized()+
      ggtitle(kr, subtitle = platform)
    return(g)
  }


plotit(actuals = dfTotalByWeek, 
       goals=baseline_dfTotal, 
       platform = "OTO + ATV + STV (base = average quantity weeks of March/19)",
       actual = "totalQTY",
       metric = "baselineQTY", 
       goal = "goalQty", 
       kr = "KR2 - Increase VAS Quantity in 10%", 
       ylabel = "Quantity VAS"
)


plotit(actuals = dfTotalByWeek, 
       goals=baseline_dfTotal, 
       platform = "OTO + ATV + STV",
       actual = "totalAmount_USD",
       metric = "baselineUSD", 
       goal = "goalAmount", 
       kr = "KR3 - Increase VAS Revenue in 5%", 
       ylabel = "VAS Revenue - Fixed USD"
)
