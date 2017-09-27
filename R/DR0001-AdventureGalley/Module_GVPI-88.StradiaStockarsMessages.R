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


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

dfStats_PE <-
  s3readRDS(object = "peru_stk_str_messanges.RDS",
            bucket = "pyrates-data-ocean/GVPI-88"
  )

dfStats_CO <-
  s3readRDS(object = "colombia_stk_str_messanges.RDS",
            bucket = "pyrates-data-ocean/GVPI-88"
  )

dfStats_IN <-
  s3readRDS(object = "india_stk_str_messanges.RDS",
            bucket = "pyrates-data-ocean/GVPI-88"
  )

dfStats_EC <-
  s3readRDS(object = "ecuador_stk_str_messanges.RDS",
            bucket = "pyrates-data-ocean/GVPI-88"
  )

# begin of UI module ----------------------------------------------------------
module_GVPI88_UI <- function(id){
  
  ns <- NS(id)
  
  fluidRow(
    box(plotOutput(ns("MessageSyncTimeIndia"))),
    box(plotOutput(ns("MessageSyncTimePeru"))),
    box(plotOutput(ns("MessageSyncTimeEcuador"))),
    box(plotOutput(ns("MessageSyncTimeColombia")))
  )
  
  # end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_GVPI88 <- function(input, output, session){

output$MessageSyncTimePeru <-
  renderPlot({
      ggplot(dfStats_PE)+
      geom_bar(stat="identity",
               aes(x = dayhour, y = perByBracket, fill = brackets)
      )+
      scale_fill_manual(values = c("darkgreen", "yellow", "red"))+
      scale_y_continuous(labels = percent)+
      scale_x_datetime(date_labels = "%Hh\n%d\n%b\n%y", date_breaks = "6 hours")+
      theme_fivethirtyeight()+
      ggtitle("Peru - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })

output$MessageSyncTimeColombia <-
  renderPlot({
    ggplot(dfStats_CO)+
      geom_bar(stat="identity",
               aes(x = dayhour, y = perByBracket, fill = brackets)
      )+
      scale_fill_manual(values = c("darkgreen", "yellow", "red"))+
      scale_y_continuous(labels = percent)+
      scale_x_datetime(date_labels = "%Hh\n%d\n%b\n%y", date_breaks = "6 hours")+
      theme_fivethirtyeight()+
      ggtitle("Colombia - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })
  

output$MessageSyncTimeIndia <-
  renderPlot({
    ggplot(dfStats_IN)+
      geom_bar(stat="identity",
               aes(x = dayhour, y = perByBracket, fill = brackets)
      )+
      scale_fill_manual(values = c("darkgreen", "yellow", "red"))+
      scale_y_continuous(labels = percent)+
      scale_x_datetime(date_labels = "%Hh\n%d\n%b\n%y", date_breaks = "6 hours")+
      theme_fivethirtyeight()+
      ggtitle("India - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })

output$MessageSyncTimeEcuador <-
  renderPlot({
    ggplot(dfStats_EC)+
      geom_bar(stat="identity",
               aes(x = dayhour, y = perByBracket, fill = brackets)
      )+
      scale_fill_manual(values = c("darkgreen", "yellow", "red"))+
      scale_y_continuous(labels = percent)+
      scale_x_datetime(date_labels = "%Hh\n%d\n%b\n%y", date_breaks = "6 hours")+
      theme_fivethirtyeight()+
      ggtitle("Ecuador - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })
  # end of SERVER module --------------------------------------------------------
}