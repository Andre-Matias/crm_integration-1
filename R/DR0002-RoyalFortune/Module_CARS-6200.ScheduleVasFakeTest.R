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
module_CARS6200_UI <- function(id){
ns <- NS(id)
  
fluidRow(
    actionButton(ns("do"), "Load Dashboard", icon = icon("sync")),
    fluidRow(
    box(solidHeader = TRUE, plotOutput(ns("ghDaily"))),
    box(solidHeader = TRUE, plotOutput(ns("ghWeekly")))
    )
)
    
# end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_CARS6200 <- function(input, output, session){

observeEvent(input$do, {
  progress <- Progress$new(session)
  progress$set(value = 0.33, message = 'Loading daily values...')
  
  dfDaily <-
    s3readRDS(object = "CARS-6200/dfDaily.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$ghDaily <-
    renderPlot({
      ggplot(dfDaily)+
        geom_line(aes(x = day ,y = CTR, color = platform ))+
        geom_point(aes(x = day ,y = CTR, color = platform ))+
        geom_text(aes(x = day ,y = CTR, label = scales::percent(round(CTR, 3)), color = platform), vjust = -0.4)+
        scale_x_datetime(date_breaks = "day")+
        scale_y_continuous(limits = c(0, NA), labels = scales::percent)+
        scale_colour_manual(values = c("#C62F1B", "#1C2B4F", "#0471CD"))+
        theme_fivethirtyeight(base_family = "opensans")+
        ggtitle("VAS Scheduler - Fake Test", subtitle = "Daily")
    })
  
  progress$set(value = 0.66, message = 'Loading total values...')
  
  dfWeekly <-
    s3readRDS(object = "CARS-6200/dfWeekly.RDS",
              bucket = "s3://pyrates-data-ocean/")
  
  output$ghWeekly <-
    renderPlot({
  ggplot(dfWeekly)+
    geom_bar( stat = "identity", aes(x = platform ,y = CTR, fill = platform ))+
    geom_text(
      aes(x = platform ,y = CTR, label = paste(scales::percent(round(CTR, 3)), "(#", ""),
          color = platform), vjust = -0.2)+
    scale_y_continuous(limits = c(0, NA), labels = scales::percent)+
    scale_fill_manual(values = c("#C62F1B", "#1C2B4F", "#0471CD"))+
    scale_color_manual(values = c("#C62F1B", "#1C2B4F", "#0471CD"))+
    ggtitle("VAS Scheduler - Fake Test", subtitle = "2018-04-02 => 2018-04-08")+
    theme_fivethirtyeight(base_family = "opensans")
    })
  
  progress$set(value = 1, message = 'Done')
  progress$close()
  })

# end of SERVER module --------------------------------------------------------
}