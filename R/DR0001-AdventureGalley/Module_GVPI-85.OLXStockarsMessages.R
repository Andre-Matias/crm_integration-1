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
library("gridExtra")
library("grid")


# load credentials ------------------------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

s3load("s3://pyrates-data-ocean/GVPI-85/AR_g.Rdata")
s3load("s3://pyrates-data-ocean/GVPI-85/EC_g.Rdata")
s3load("s3://pyrates-data-ocean/GVPI-85/PE_g.Rdata")
s3load("s3://pyrates-data-ocean/GVPI-85/CO_g.Rdata")



# begin of UI module ----------------------------------------------------------
module_GVPI85_UI <- function(id){
  
  ns <- NS(id)
  
  fluidRow(
    box(plotOutput(ns("AR_g"))),
    box(plotOutput(ns("CO_g"))),
    box(plotOutput(ns("EC_g"))),
    box(plotOutput(ns("PE_g")))
  )
  
  # end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_GVPI85 <- function(input, output, session){
  
  output$AR_g <- renderPlot({
    grid.draw(AR_g)
  })
  
  output$PE_g <- renderPlot({
    grid.draw(PE_g)
  })
  
  output$EC_g <- renderPlot({
    grid.draw(EC_g)
  })
  
  output$CO_g <- renderPlot({
    grid.draw(CO_g)
  })
  # end of SERVER module --------------------------------------------------------
}