library(shiny)
library(jsonlite)
library(googleVis)
shinyUI(fluidPage(
  titlePanel("Main metrics comparison"),
  sidebarPanel(
    selectInput("sitecode","Site - only olxpl active",
               c("otomoto.pl" = "otomotopl","otodom.pl" = "otodompl","olx.pl" = "olxpl"),
               selected = "olxpl"),
    helpText("ATI numbers are not the olx.pl ones, it's just for the example"),
    selectInput("platforms","Platforms",
                c("Android" = "android","iOS" = "ios", "Desktop" = "desktop", "Responsive" = "i2", "Dunno" = "undefined"),
                selected = "desktop"
                ),
    helpText("Only desktop implemented now"),
    dateRangeInput("dates", 
                   "Date range",
                   start = "2017-01-01", 
                   end = as.character(Sys.Date()-1)),
    submitButton("1- Update data"),
    br(),
    downloadButton("downloadcsv","2- Then you can download daily data"),
    br(),
    br(),
    h4("APIs"),
    htmlOutput("debug"),
    br(),
    helpText("Mixpanel pv JQL"),
    verbatimTextOutput("api_mp_pv"),
    helpText("Mixpanel dau JQL"),
    verbatimTextOutput("api_mp_dau"),
    helpText("Mixpanel au JQL"),
    verbatimTextOutput("api_mp_au"),
    helpText("ATI pv API"),
    verbatimTextOutput("api_ati_pv"),
    helpText("ATI dau API"),
    verbatimTextOutput("api_ati_dau"),
    helpText("ATI au API"),
    verbatimTextOutput("api_ati_au")
  ),
  mainPanel(
    htmlOutput("activeusers"),
    column(
      6,
      h3("Mixpanel"),
      h4("Page views"),
      htmlOutput("chart_mp_pv"),
      h4("Daily active users"),
      htmlOutput("chart_mp_dau")
    ),
    column(
      6,
      h3("ATI"),
      h4("Page views"),
      htmlOutput("chart_ati_pv"),
      h4("Daily active users"),
      htmlOutput("chart_ati_dau")
    )
  )
))