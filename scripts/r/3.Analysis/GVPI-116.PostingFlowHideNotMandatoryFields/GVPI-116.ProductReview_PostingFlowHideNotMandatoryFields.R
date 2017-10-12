# libraries -------------------------------------------------------------------
library("config")
library("RMySQL")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("lubridate")

# Load personal credentials ---------------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")

dfAll <- data.frame()

for(site in c("standvirtual_pt", "otomoto_pl", "autovit_ro")){
  
dfTemp <-data.frame()
print(site)
# get database config
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", site)
)

# define query ----------------------------------------------------------------

cmdSqlQuery <-
  "
  SELECT A.id, A.created_at_first, A.description, B.name_en
    FROM ads A
  INNER JOIN categories B
  ON A.category_id = B.id
  WHERE 
    created_at_first >= '2017-05-01 00:00:00'
    AND created_at_first < CURRENT_DATE()
    AND net_ad_counted = 1
    AND B.parent_id = 0
UNION ALL
  SELECT A.id, A.created_at_first, A.description, C.name_en
    FROM ads A
  INNER JOIN categories B
  ON A.category_id = B.id
  INNER JOIN categories C
  ON B.parent_id = C.id
  WHERE 
    created_at_first >= '2017-05-01 00:00:00'
    AND created_at_first < CURRENT_DATE()
    AND net_ad_counted = 1
    AND B.parent_id != 0
  ;
  "

# connect to db ---------------------------------------------------------------
conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= bi_team_pt_password,
                  host= ifelse(Sys.info()["nodename"] =="bisb",
                               "127.0.0.1",
                               config$DbHost),
                  port = ifelse(Sys.info()["nodename"] =="bisb",
                               config$BiServerPort,
                               config$DbPort),
                  dbname = config$DbName
)


# get data --------------------------------------------------------------------
dfQueryResults <- 
  dbGetQuery(conDB,cmdSqlQuery)

dbDisconnect(conDB)

# summarise data --------------------------------------------------------------

dfTemp <-
  dfQueryResults %>%
  mutate(
    platform = site, 
    day = as.Date(created_at_first),
    week = floor_date(day, "week"),
    DescriptionState = ifelse(!is.na(description) 
                              & nchar(description, type = "bytes") > 1,
                              "with description",
                              "without description")
  ) %>%
  group_by(platform, day, week, name_en, DescriptionState) %>%
  summarise(qtyListings = sum(n()))

if(nrow(dfAll)==0){
    dfAll <- dfTemp}
  else{
    dfAll <- rbind(dfAll, dfTemp)
  }
}

# filter only cars category

df <-
  dfAll %>%
  filter(name_en=="Cars") %>%
  group_by(platform, day, DescriptionState) %>%
  summarise(qtyLystingByDay = sum(qtyListings)) %>%
  arrange(platform, day, DescriptionState) %>%
  mutate(perLystingByDay = qtyLystingByDay / sum(qtyLystingByDay)) 
# %>%
#  filter(DescriptionState == 'without description')

# save it to amazon s3 --------------------------------------------------------

s3saveRDS(x = dfAll,
          object = "DescriptionState.RDS",
          bucket = "pyrates-data-ocean/GVPI-116")

gh <-
  ggplot(df)+
  geom_smooth(aes(x = day, y = perLystingByDay, color = platform), se = FALSE)+
  scale_x_date(limits = c(as.Date("2017-07-01"), as.Date("2017-10-15")))+
  scale_y_continuous(limits = c(0, 0.20), labels = percent)+
  ggtitle("% Cars' Listings without Description")+
  theme_fivethirtyeight()



# liquidity 1 messages in 7 seven days

## Carspt 
dfLiquidityCarspt <-
  read.csv2(
    "~/verticals-bi/scripts/r/3.Analysis/GVPI-116.PostingFlowHideNotMandatoryFields/carspt.txt",
    sep = "\t",
    dec = ".",
    header = TRUE
    )

dfLiquidityCarspt$created_at_first_day <-
  as.Date(dfLiquidityCarspt$created_at_first_day)

ghLiquidityCarspt <-
ggplot(data = dfLiquidityCarspt)+
  geom_smooth(aes(x = created_at_first_day,
                y = perlistingswithliquidity,
                group = blank_description,
                color = blank_description
                ),
              se = FALSE
            )+
  scale_y_continuous(limits = c(0, 0.40), labels = percent)+
  scale_x_date(limits = c(as.Date("2017-07-01"), as.Date("2017-10-01")))+
  theme_fivethirtyeight() + theme(legend.position="none")+
  ggtitle("Standvirtual")

## otomotopl


dfLiquidityOtomotoPL <-
  read.csv2(
    "~/verticals-bi/scripts/r/3.Analysis/GVPI-116.PostingFlowHideNotMandatoryFields/otomotopl.txt",
    sep = "\t",
    dec = ".",
    header = TRUE
  )

dfLiquidityOtomotoPL$created_at_first_day <-
  as.Date(dfLiquidityOtomotoPL$created_at_first_day)

ghLiquidityOtomotoPL <- 
ggplot(data = dfLiquidityOtomotoPL)+
  geom_smooth(aes(x = created_at_first_day,
                  y = perlistingswithliquidity,
                  group = blank_description,
                  color = blank_description
  ), se = FALSE
  )+
  scale_y_continuous(limits = c(0, 0.15), labels = percent)+
  scale_x_date(limits = c(as.Date("2017-07-01"), as.Date("2017-10-01")))+
  theme_fivethirtyeight() + theme(legend.position="none")+
  ggtitle("Otomoto")




## autovit


dfLiquidityAutovitRO <-
  read.csv2(
    "~/verticals-bi/scripts/r/3.Analysis/GVPI-116.PostingFlowHideNotMandatoryFields/autovitro.txt",
    sep = "\t",
    dec = ".",
    header = TRUE
  )

dfLiquidityAutovitRO$created_at_first_day <-
  as.Date(dfLiquidityAutovitRO$created_at_first_day)

ghLiquidityAutovitRO <-
ggplot(data = dfLiquidityAutovitRO)+
  geom_smooth(aes(x = created_at_first_day,
                  y = perlistingswithliquidity,
                  group = blank_description,
                  color = blank_description
  ), se = FALSE
  )+
  scale_y_continuous(limits = c(0, 0.25), labels = percent)+
  scale_x_date(limits = c(as.Date("2017-07-01"), as.Date("2017-10-01")))+
  theme_fivethirtyeight() + theme(legend.position="none")+
  ggtitle("Autovit")


f <-
  grid.arrange( ncol = 3, nrow = 1,
    ghLiquidityOtomotoPL, ghLiquidityAutovitRO, ghLiquidityCarspt
  )