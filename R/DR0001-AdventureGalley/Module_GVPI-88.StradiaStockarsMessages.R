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
  s3readRDS(object = "peru_stk_str_messages.RDS",
            bucket = "pyrates-data-ocean/GVPI-88"
  )

dfStatsInOut_PE <-
  s3readRDS(object = "peru_dfStatsInOut.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStats_CO <-
  s3readRDS(object = "colombia_stk_str_messages.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStatsInOut_CO <-
  s3readRDS(object = "colombia_dfStatsInOut.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")


dfStats_IN <-
  s3readRDS(object = "india_stk_str_messages.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStatsInOut_IN <-
  s3readRDS(object = "india_dfStatsInOut.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")


dfStats_EC <-
  s3readRDS(object = "ecuador_stk_str_messages.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStatsInOut_EC <-
  s3readRDS(object = "ecuador_dfStatsInOut.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStats_AR <-
  s3readRDS(object = "argentina_stk_str_messages.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

dfStatsInOut_AR <-
  s3readRDS(object = "argentina_dfStatsInOut.RDS",
            bucket = "pyrates-data-ocean/GVPI-88")

# begin of UI module ----------------------------------------------------------
module_GVPI88_UI <- function(id){
  
  ns <- NS(id)
  
  fluidRow(
    box(plotOutput(ns("MessageSyncTimeIndia"))),
    box(plotOutput(ns("QtyInOutIndia"))),
    box(plotOutput(ns("MessageSyncTimeArgentina"))),
    box(plotOutput(ns("QtyInOutArgentina"))),
    box(plotOutput(ns("MessageSyncTimePeru"))),
    box(plotOutput(ns("QtyInOutPeru"))),
    box(plotOutput(ns("MessageSyncTimeEcuador"))),
    box(plotOutput(ns("QtyInOutEcuador"))),
    box(plotOutput(ns("MessageSyncTimeColombia"))),
    box(plotOutput(ns("QtyInOutColombia")))
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
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
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
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
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
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
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
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      theme_fivethirtyeight()+
      ggtitle("Ecuador - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })

output$MessageSyncTimeArgentina <-
  renderPlot({
    ggplot(dfStats_AR)+
      geom_bar(stat="identity",
               aes(x = dayhour, y = perByBracket, fill = brackets)
      )+
      scale_fill_manual(values = c("darkgreen", "yellow", "red"))+
      scale_y_continuous(labels = percent)+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      theme_fivethirtyeight()+
      ggtitle("Argentina - Stockars/Stradia Messages Syncing Time ",
              subtitle = "seconds")
  })

output$QtyInOutIndia <-
  renderPlot({
    ggplot(dfStatsInOut_IN)+
      geom_bar(stat = "identity",
               aes(x = day, y = qtyByDirection, fill = direction),
               position = "dodge")+
      geom_text(aes(x = day, y = qtyByDirection,
                    label = qtyByDirection,
                    color = direction, group = direction),
                position = position_dodge(width=0.9),
                vjust = 0
      )+
      theme_fivethirtyeight()+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      ggtitle("India - Quantity Messages")
  })


output$QtyInOutArgentina <-
  renderPlot({
    ggplot(dfStatsInOut_AR)+
      geom_bar(stat = "identity",
               aes(x = day, y = qtyByDirection, fill = direction),
               position = "dodge")+
      geom_text(aes(x = day, y = qtyByDirection,
                    label = qtyByDirection,
                    color = direction, group = direction),
                position = position_dodge(width=0.9),
                vjust = 0
      )+
      theme_fivethirtyeight()+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      ggtitle("Argentina - Quantity Messages")
  })

output$QtyInOutPeru <-
  renderPlot({
    ggplot(dfStatsInOut_PE)+
      geom_bar(stat = "identity",
               aes(x = day, y = qtyByDirection, fill = direction),
               position = "dodge")+
      geom_text(aes(x = day, y = qtyByDirection,
                    label = qtyByDirection,
                    color = direction, group = direction),
                position = position_dodge(width=0.9),
                vjust = 0
      )+
      theme_fivethirtyeight()+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      ggtitle("Peru - Quantity Messages")
  })

output$QtyInOutEcuador <-
  renderPlot({
    ggplot(dfStatsInOut_EC)+
      geom_bar(stat = "identity",
               aes(x = day, y = qtyByDirection, fill = direction),
               position = "dodge")+
      geom_text(aes(x = day, y = qtyByDirection,
                    label = qtyByDirection,
                    color = direction, group = direction),
                position = position_dodge(width=0.9),
                vjust = 0
      )+
      theme_fivethirtyeight()+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      ggtitle("Ecuador - Quantity Messages")
  })

output$QtyInOutColombia <-
  renderPlot({
    ggplot(dfStatsInOut_CO)+
      geom_bar(stat = "identity",
               aes(x = day, y = qtyByDirection, fill = direction),
               position = "dodge")+
      geom_text(aes(x = day, y = qtyByDirection,
                    label = qtyByDirection,
                    color = direction, group = direction),
                position = position_dodge(width=0.9),
                vjust = 0
      )+
      theme_fivethirtyeight()+
      scale_x_date(date_labels = "%d\n%b\n%Y", date_breaks = "days")+
      ggtitle("Colombia - Quantity Messages")
  })

  # end of SERVER module --------------------------------------------------------
}