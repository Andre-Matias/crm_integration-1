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

rawDf <-
  s3readRDS(object = "DescriptionState.RDS",
            bucket = "pyrates-data-ocean/GVPI-116"
  )

# begin of UI module ----------------------------------------------------------
module_CARS5999_UI <- function(id){
  
  ns <- NS(id)
  
  fluidRow(
    box(plotOutput(ns("graph")))
    # box(checkboxGroupInput(
    #   inputId = ns("gvpi116SelectPlatform"),
    #   label = "Project",
    #   choices = unique(rawDf$platform)
    #   )),
    # 
    # box(checkboxGroupInput(
    #   inputId = ns("gvpi116SelectCategories"),
    #   label = "Category",
    #   choices = unique(rawDf$name_en)
    #   ))
  )
    
# end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_CARS5999 <- function(input, output, session){

output$graph <-
  renderPlot({
  ggplot(CARS5999_df)+geom_line(aes(day, CTR, group = platform, color = platform))+scale_x_datetime(date_breaks = "day")
  })

# end of SERVER module --------------------------------------------------------
}