# load libraries --------------------------------------------------------------
library("shiny")
library("shinydashboard")
library("aws.s3")
library("ggplot2")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")
library("scales")
library("ggthemes")
library("ggplot2")
library("plotly")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# begin of UI module ----------------------------------------------------------
module_CARS5999_UI <- function(id){
ns <- NS(id)
  
fluidRow(
    actionButton(ns("do"), "Load Dashboard", icon = icon("sync")),
    fluidRow(
    box(solidHeader = TRUE, plotOutput(ns("DAU_OtomotoPL"))),
    box(solidHeader = TRUE, plotOutput(ns("WAU_OtomotoPL"))),
    box(solidHeader = TRUE, plotOutput(ns("per_WAU_OtomotoPL")))
    ),
    fluidRow(
    box(solidHeader = TRUE, plotOutput(ns("DAU_AutovitRO"))),
    box(solidHeader = TRUE, plotOutput(ns("WAU_AutovitRO"))),
    box(solidHeader = TRUE, plotOutput(ns("per_WAU_AutovitRO")))
    ),
    fluidRow(
    box(solidHeader = TRUE, plotOutput(ns("DAU_StandvirtualPT"))),
    box(solidHeader = TRUE, plotOutput(ns("WAU_StandvirtualPT"))),
    box(solidHeader = TRUE, plotOutput(ns("per_WAU_StandvirtualPT")))
    )
)
    
# end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_CARS5999 <- function(input, output, session){

observeEvent(input$do, {
  progress <- Progress$new(session)
  progress$set(value = 0.15, message = 'Loading Otomoto DAU')
  
  dfDAU_OtomotoPL <-
    s3readRDS(object = "CARS-5999/dfDAU_OtomotoPL.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$DAU_OtomotoPL <-
    renderPlot({
      ggplot(dfDAU_OtomotoPL)+
        geom_bar(stat = "identity",
                 aes(x = day, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        ggtitle("Daily Unique Sellers Using Ad Ranking", subtitle = "Otomoto.PL")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 0.30, message = 'Loading Otomoto WAU')
  
  dfWAU_OtomotoPL <-
    s3readRDS(object = "CARS-5999/dfWAU_OtomotoPL.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$WAU_OtomotoPL <-
    renderPlot({
      ggplot(dfWAU_OtomotoPL)+
        geom_bar(stat = "identity",
                 aes(x = week, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%y")+
        ggtitle("Weekly Unique Sellers Using Ad Ranking", subtitle = "Otomoto.PL")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  output$per_WAU_OtomotoPL <-
    renderPlot({
      ggplot(dfWAU_OtomotoPL)+
        geom_line(aes(x = week, y = CTR, group = platform, color = platform ))+
        geom_point(aes(x = week, y = CTR, group = platform, color = platform ))+
        scale_x_datetime(date_breaks = "week", date_labels = "%d\n%b\n%y")+
        scale_y_continuous(labels = scales::percent, limits = c(0, NA))+
        ggtitle("% Weekly Active Sellers using Ads Ranking", subtitle = "Otomoto.PL")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 0.45, message = 'Loading Autovit DAU')
  
  dfDAU_AutovitRO <-
    s3readRDS(object = "CARS-5999/dfDAU_AutovitRO.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$DAU_AutovitRO <-
    renderPlot({
      ggplot(dfDAU_AutovitRO)+
        geom_bar(stat = "identity",
                 aes(x = day, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        ggtitle("Daily Unique Sellers Using Ad Ranking", subtitle = "Autovit.RO")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 0.60, message = 'Loading Autovit WAU')
  
  dfWAU_AutovitRO <-
    s3readRDS(object = "CARS-5999/dfWAU_AutovitRO.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$WAU_AutovitRO <-
    renderPlot({
      ggplot(dfWAU_AutovitRO)+
        geom_bar(stat = "identity",
                 aes(x = week, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        ggtitle("Weekly Unique Sellers Using Ad Ranking", subtitle = "Autovit.RO")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  output$per_WAU_AutovitRO <-
    renderPlot({
      ggplot(dfWAU_AutovitRO)+
        geom_line(aes(x = week, y = CTR, group = platform, color = platform ))+
        geom_point(aes(x = week, y = CTR, group = platform, color = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        scale_y_continuous(labels = scales::percent, limits = c(0, NA))+
        ggtitle("% Weekly Active Sellers using Ads Ranking", subtitle = "Autovit.RO")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 0.75, message = 'Loading Standvirtual DAU')
  
  dfDAU_StandvirtualPT <-
    s3readRDS(object = "CARS-5999/dfDAU_StandvirtualPT.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$DAU_StandvirtualPT <-
    renderPlot({
      ggplot(dfDAU_StandvirtualPT)+
        geom_bar(stat = "identity",
                 aes(x = day, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        ggtitle("Daily Unique Sellers Using Ad Ranking", subtitle = "Standvirtual.PT")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 0.90, message = 'Loading Standvirtual WAU')
  
  dfWAU_StandvirtualPT <-
    s3readRDS(object = "CARS-5999/dfWAU_StandvirtualPT.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$WAU_StandvirtualPT <-
    renderPlot({
      ggplot(dfWAU_StandvirtualPT)+
        geom_bar(stat = "identity",
                 aes(x = week, y = totalUniqueMyAdsRankingRefresh, fill = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        ggtitle("% Weekly Active Sellers using Ads Ranking", subtitle = "Standvirtual.PT")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  output$per_WAU_StandvirtualPT <-
    renderPlot({
      ggplot(dfWAU_StandvirtualPT)+
        geom_line(aes(x = week, y = CTR, group = platform, color = platform ))+
        geom_point(aes(x = week, y = CTR, group = platform, color = platform ))+
        scale_x_datetime(date_breaks = "day", date_labels = "%d\n%b\n%y")+
        scale_y_continuous(labels = scales::percent, limits = c(0, NA))+
        ggtitle("Weekly Unique Sellers Using Ad Ranking", subtitle = "Standvirtual.PT")+
        theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 1, message = 'Done')
  progress$close()
  })

# end of SERVER module --------------------------------------------------------
}