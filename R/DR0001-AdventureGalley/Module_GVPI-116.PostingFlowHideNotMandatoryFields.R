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
module_GVPI116_UI <- function(id){
  
  ns <- NS(id)
  
  fluidRow(
    box(plotOutput(ns("HideDescriptionPlot"))),
    
    box(checkboxGroupInput(
      inputId = ns("gvpi116SelectPlatform"),
      label = "Project",
      choices = unique(rawDf$platform)
      )),
    
    box(checkboxGroupInput(
      inputId = ns("gvpi116SelectCategories"),
      label = "Category",
      choices = unique(rawDf$name_en)
      ))
  )
    
# end of UI module ------------------------------------------------------------
}

# begin of SERVER module ------------------------------------------------------
module_GVPI116 <- function(input, output, session){

df <-
  reactive({

  tmp <-
  rawDf%>%
  filter(platform %in% input$gvpi116SelectPlatform
         & name_en %in% input$gvpi116SelectCategories ) %>%
  group_by(day, DescriptionState) %>%
  summarise(qtyListings = sum(qtyListings)) %>%
  mutate(
    perListings = qtyListings / sum(qtyListings)
  )
  tmp
  })


output$HideDescriptionPlot <-
  renderPlot({
  ggplot(df())+
  geom_line(
    aes(x = day, y = perListings,
        group = DescriptionState, color = DescriptionState))+
  scale_y_continuous(labels = percent, limits = c(0,1))+
  theme_fivethirtyeight()+
  theme(legend.position="bottom")+
  guides(col = guide_legend(nrow = 1, bycol = TRUE))+
  ggtitle("Description Field Usage on Posting Ad Flow",
          subtitle = paste(
            "Project:",
            paste(input$gvpi116SelectPlatform, collapse = " "),
            "| Categories: ",
            paste(input$gvpi116SelectCategories, collapse = ", ")
          )
  )
  })

# end of SERVER module --------------------------------------------------------
}