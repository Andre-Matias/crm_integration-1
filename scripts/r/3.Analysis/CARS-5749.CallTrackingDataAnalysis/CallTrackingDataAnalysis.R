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
library("lubridate")

# load mixpanel user's credentials --------------------------------------------
load("~/iovox.Rdata")
font_add_google("Open Sans", "opensans")

# get data from IOVOX ---------------------------------------------------------
for(i in names(keysIOVOX)){
  print(keysIOVOX[i])
query <- "https://api.iovox.com:444/Calls?v=3&method=getCallData&output=JSON"
getdata<-GET(
  url=query, 
  add_headers(username=as.character(i) , secureKey=as.character(keysIOVOX[i]))
  )

a <- httr::content(getdata, as = "text")
b <- fromJSON(a)
c <- b$response$results %>% as.data.frame
c$site <- as.character(i)
assign(paste0("dfRaw", as.character(i)), as_tibble(c))
}
rm("a", "b", "c", "getdata", "i", "keysIOVOX", "query")

# clean data ------------------------------------------------------------------

rawdfs <- ls(pattern = "dfRaw")

for(i in rawdfs){
  df <- get(i)
  df$day <- as.Date(df$result.call_start)
  i <- gsub("Raw", "Odl", i)
  assign(i, df)
}

odldfs <- ls(pattern = "dfOdl")


for(i in odldfs){
  df <- get(i)
  
  dfStats <-
    df %>%
    group_by(site, day, result.call_result) %>%
    summarise(qtyByType = sum(n())) %>%
    group_by(site, day) %>%
    mutate(qtyByDay = sum(qtyByType),
           perByType = qtyByType / qtyByDay)

  i <- gsub("Odl", "Stats", i)
  
  assign(i, dfStats)
}

statsdfs <- ls(pattern = "dfStats")

for(i in statsdfs){
  
dfGraph <- get(i)

siteGraph <- unique(dfGraph$site)
startDateGraph <- min(dfGraph$day)
endDateGraph <- max(dfGraph$day)

ghQtyResultByDay <-
  ggplot(dfGraph)+
    geom_bar(
      stat="identity", 
      aes(x = day ,y = qtyByType, fill = result.call_result, group = result.call_result)
      )+
    scale_x_date(date_breaks = "day", date_labels = "%d\n%b\n%y")+
    theme_fivethirtyeight(base_family = "opensans")+
    ggtitle("Calltracking - Calls By Result", 
            subtitle = paste(siteGraph, "||", startDateGraph,"to", endDateGraph)
            )

ghPerResultByDay <-
  ggplot(dfGraph)+
    geom_line(
      aes(x = day, y = perByType, color = result.call_result, group = result.call_result)
      )+
    scale_x_date(date_breaks = "day", date_labels = "%d\n%b\n%y")+
    scale_y_continuous(limits = c(0, 1), labels = scales::percent)+
    theme_fivethirtyeight(base_family = "opensans")+
    ggtitle("Calltracking - Calls Share By Result ", 
            subtitle = paste(siteGraph, "||", startDateGraph,"to", endDateGraph)
    )

ghQtyByDay <-
  ggplot(dfGraph)+
    geom_line(
      aes(x = day, y = qtyByDay)
    )+
    scale_x_date(date_breaks = "day", date_labels = "%d\n%b\n%y")+
    scale_y_continuous(limits = c(0, NA), breaks = scales::pretty_breaks())+
    theme_fivethirtyeight(base_family = "opensans")+
    ggtitle("Calltracking - Total Calls Generated", 
            subtitle = paste(siteGraph, "||", startDateGraph,"to", endDateGraph)
    )

  i <- gsub("dfStats", "", i)
  
  assign(paste0("ghQtyResultByDay", i), ghQtyResultByDay)
  assign(paste0("ghPerResultByDay", i), ghPerResultByDay)
  assign(paste0("ghQtyByDay", i), ghQtyByDay)
}