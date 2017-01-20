library(shiny)
library(jsonlite)
library(reshape)
library(googleVis)
library(gsubfn)
library(stringr)

shinyUI(fluidPage(
  titlePanel("Users flow"),
  sidebarPanel(
    selectInput("event","From where ?",
                c("ad page" = "ad_page","listing" = "listing", "home page" = "home"),
                selected = "adpage"),
    selectInput("platforms","Platforms",
                c("Android" = "android","iOS" = "ios", "Desktop" = "desktop", "Responsive" = "i2", "Dunno" = "undefined"),
                selected = "desktop"
    ),
    sliderInput("depth","What's the depth ?",3, min = 2, max = 5),
    numericInput("nb_users","How many min user in a flow ?",10,min = 1,max = 80),
    dateRangeInput("dates", 
                   "Date range",
                   start = "2017-01-01", 
                   end = as.character(Sys.Date()-1)),
    submitButton("Update data"),
    br(),
    downloadButton("download_sankeydata","Download the data (csv)"),
    br(),
    br(),
    verbatimTextOutput("api"),
    width = 3
  ),
  mainPanel(
    htmlOutput("sankey")
  )
))