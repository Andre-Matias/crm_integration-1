library("data.table")
library("RMySQL")
library("lubridate")
library("glue")
library("ggplot2")
library("ggthemes")

# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

rm(list=setdiff(ls(), c("bi_team_pt_password")))

runDate <- Sys.Date()
runDateMonth <- format(runDate, "%Y-%m")
lastMonthStart <- floor_date(as.Date(runDate), "month") - months(1)
lastMonthEnd <- floor_date(as.Date(runDate), "day") - months(1)
runDatePreviousMonth <- format(lastMonthStart, "%Y-%m")

# connect to database  ------------------------------------------------------
  conDB <-  
    dbConnect(
      RMySQL::MySQL(),
      username = "bi_team_pt",
      password = bi_team_pt_password,
      host = "127.0.0.1",
      port = 3317, 
      dbname = "otomotopl"
    )
  
# get data ------------------------------------------------------------------

dbSqlQuery <-
  "
  SELECT 
  DATE_FORMAT(fi.created_at, '%Y-%m')as month, 
  COUNT(distinct ub.id)qtyPPU, 
  SUM(total_gross_amount)sumGrossAmount
  FROM sap_invoices fi
  INNER JOIN users_business ub on ub.id = fi.user_id
  WHERE fi.created_at >= '2018-01-01'
  and fi.total_gross_amount > 0
  and not (
    ub.email LIKE '%@autovit%' 
    OR ub.email LIKE '%sunfra.%' 
    OR ub.email LIKE '%@olx.%' 
    OR ub.email LIKE '%@tablica.%' 
    OR ub.email LIKE '%@fixeads.%' 
    OR ub.email LIKE'%@otomoto.%'
    OR ub.email LIKE '%@otodom.%' 
    OR ub.email LIKE '%@slando.%'
  )
  group by 1
  order by 1 ASC
  ;
  "

dbSqlQuery2 <-
    "
    SELECT 
    DATE_FORMAT(fi.created_at, '%Y-%m')as month, 
    COUNT(distinct ub.id)qtyPPU, 
    SUM(total_gross_amount)sumGrossAmount
    FROM sap_invoices fi
    INNER JOIN users_business ub on ub.id = fi.user_id
    WHERE fi.created_at >= '{lastMonthStart}' AND fi.created_at < '{lastMonthEnd}'
    and fi.total_gross_amount > 0
    and not (
    ub.email LIKE '%@autovit%' 
    OR ub.email LIKE '%sunfra.%' 
    OR ub.email LIKE '%@olx.%' 
    OR ub.email LIKE '%@tablica.%' 
    OR ub.email LIKE '%@fixeads.%' 
    OR ub.email LIKE'%@otomoto.%'
    OR ub.email LIKE '%@otodom.%' 
    OR ub.email LIKE '%@slando.%'
    )
    group by 1
    order by 1 ASC
    ;
    "


dfSqlQuery <-
  dbGetQuery(conDB, dbSqlQuery)

dbSqlQuery2 <- glue(dbSqlQuery2)

dfSqlQuery2 <-
  dbGetQuery(conDB, dbSqlQuery2)

# disconnect from database  -------------------------------------------------
dbDisconnect(conDB)

# get the baseline for PPU and Revenue

dfMonth <- 
  dfSqlQuery %>%
  group_by(month) %>%
  summarise(
    qtyPPU = sum(qtyPPU),
    sumGrossAmount = sum(sumGrossAmount)
      ) %>%
  arrange(month) %>%
  mutate_at(vars(qtyPPU, sumGrossAmount), funs(chg = ((.-lag(.))/lag(.))))

dfMonth[dfMonth$month==runDateMonth, c("qtyPPU_chg")] <-
  dfMonth[dfMonth$month==runDateMonth, c("qtyPPU")] / dfSqlQuery2[dfSqlQuery2$month==runDatePreviousMonth, c("qtyPPU")] -1 

dfMonth[dfMonth$month==runDateMonth, c("sumGrossAmount_chg")] <-
  dfMonth[dfMonth$month==runDateMonth, c("sumGrossAmount")] / dfSqlQuery2[dfSqlQuery2$month==runDatePreviousMonth, c("sumGrossAmount")] -1 

baselineMeanPPU <- 
  mean(
    dfMonth$qtyPPU_chg[dfMonth$month %in% c("2018-01", "2018-02", "2018-03", "2018-04", "2018-05", "2018-06")], na.rm = TRUE)

baselineMeanGrossAmount <- 
  mean(
    dfMonth$sumGrossAmount_chg[dfMonth$month %in% c("2018-01", "2018-02", "2018-03", "2018-04", "2018-05", "2018-06")], na.rm = TRUE)

dfMonth$month <- as.Date(paste0(dfMonth$month,"-01"), "%Y-%m-%d")

ggplot(dfMonth)+
  geom_line(aes(month, qtyPPU_chg))+
  geom_hline(yintercept=baselineMeanPPU, color = "red")+
  scale_y_continuous(labels = scales::percent)+
  ggtitle("OKR - PPU Growth Rate", subtitle = "Otomoto || baseline = mean(2018/01:2018/06)")+
  theme_economist_white()
  
ggplot(dfMonth)+
  geom_line(aes(month, sumGrossAmount_chg))+
  geom_hline(yintercept = baselineMeanGrossAmount, color = "red")+
  scale_y_continuous(labels = scales::percent)+
  ggtitle("OKR - Transactional Revenue Growth Rate", subtitle = "Otomoto || baseline = mean(2018/01:2018/06)")+
  theme_economist_white()

