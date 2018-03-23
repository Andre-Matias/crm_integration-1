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
  fluidPage(
    actionButton(ns("do"), "Load Dashboard"),
    box(renderPlot(ns("DAU_OtomotoPL")))
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
        scale_x_datetime(date_breaks = "day")
    })
      
      
  progress$set(value = 0.30, message = 'Loading Otomoto WAU')
  progress$set(value = 0.45, message = 'Loading Autovit DAU')
  progress$set(value = 0.60, message = 'Loading Autovit WAU')
  progress$set(value = 0.75, message = 'Loading Standvirtual DAU')
  progress$set(value = 0.90, message = 'Loading Standvirtual wAU')
  progress$close()
  })

# end of SERVER module --------------------------------------------------------
}