# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("showtext")
library("glue")
library("aws.s3")
library("lubridate")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
load("~/GlobalConfig.Rdata")
rm(list = ls(pattern = "df"))

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

font_add_google("Open Sans", "opensans")

startDate <- as.character("2018-03-01")
endDate <- as.character(Sys.Date())

listAccounts <- 
  list(otomotopl = list("OtomotoPL", mixpanelOtomotoAccount, 3317, "otomotopl"),
       autovitro = list("AutovitRO", mixpanelAutovitAccount, 3315, "autovitro"),
       standvirtualpt = list("StandvirtualPT", mixpanelStandvirtualAccount, 3308, "carspt")
  )

jqlQueryNotBump <- 
  '
    function main() {{
    return Events({{
    from_date: "{startDate}",
    to_date:   "{endDate}"
    }})
    .filter(e => e.name.includes("my_ads_bulk_vas_modal")
            && (["topads", "bump_up", "highlight", "ad_homepage", "ad_bighomepage"].indexOf(e.properties.vas_type) >= 0)
    )
    .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name", "properties.f_order_id", "properties.vas_type"], mixpanel.reducer.count());
    }}
  '

jqlQueryBump <- 
  '
    function main() {{
    return Events({{
    from_date: "{startDate}",
    to_date:   "{endDate}"
    }})
    .filter(e => e.name.includes("my_ads_bulk_vas_modal")
    // && e.properties.vas_type == "bump_up"
    )
    .groupBy([mixpanel.numeric_bucket("time", mixpanel.daily_time_buckets), "name", "properties.f_order_id", "properties.vas_type.0"], mixpanel.reducer.count());
    }}
  '

jqlQueryNotBump <-
  glue(jqlQueryNotBump)

jqlQueryBump <-
  glue(jqlQueryBump)


dfVAS_all <- as_tibble()

for(i in listAccounts){
  
  dfJqlQueryNotBump <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQueryNotBump,
                       columnNames = c("epoch_day", "event", "f_order_id", "vas_type", "quantity"),
                       toNumeric = c(1, 3, 5) 
      )
    )

  dfJqlQueryBump <- 
    as_tibble(
      mixpanelJQLQuery(account = i[[2]], 
                       jqlQueryBump,
                       columnNames = c("epoch_day", "event", "f_order_id", "vas_type", "quantity"),
                       toNumeric = c(1, 3, 5) 
      )
    )
  
  dfVAS <- rbind(dfJqlQueryNotBump, dfJqlQueryBump)
  
  assign(x = paste0("dfVAS_", as.character(i[[1]])),
         value = dfVAS
  )
  
  dfVAS$project <- as.character(i[[1]])
  
  if(nrow(dfVAS_all)==0){
    dfVAS_all <- dfVAS
  } else {
    dfVAS_all <- rbind(dfVAS_all, dfVAS)
  }
  
  orders <- 
    unique(dfVAS$f_order_id[!is.na(dfVAS$f_order_id)])
  
}



df <-
  dfVAS_all %>%
  mutate(day = anytime(as.numeric(epoch_day)/1000)) %>%
  select(-epoch_day) %>%
  mutate(week = floor_date(day, unit = "week")) %>%
  group_by(project, day, vas_type, event) %>%
  summarise(totalQuantity = sum(quantity, na.rm = TRUE)) %>%
  spread(key = event, value = totalQuantity)