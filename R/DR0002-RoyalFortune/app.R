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
library("plotly")


# load mixpanel user's credentials --------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# load modules ----------------------------------------------------------------
source("Module_CARS-5999.PercentageOfUsersThatSeeAdsRanking.R")

ui <- 
  dashboardPage(
  dashboardHeader(title = "Royal Fortune"),

# dashboardSidebar start ------------------------------------------------------

  dashboardSidebar(
      sidebarMenu(
        menuItem("Global", tabName = 'tabGlobal', icon = icon('dashboard')),
        menuItem("Monetization", tabName = 'tabMonetization', icon = icon('money')
                 )
      ),
      sidebarMenu(
        menuItem("Pro Tools", tabName = "bt", icon = icon("wrench"),
                 menuItem('OKR', tabName = 'tabProtoolsOKR',
                          menuSubItem('FY18Q4 Ad Ranking Usage',
                                      tabName = 'tabFY2018AdsRanking'
                                      )
                          )
        )
      )
  ),
# dashboardSidebar end --------------------------------------------------------

# dashboardbody start ---------------------------------------------------------
  dashboardBody(
    tabItems(
      # Global 
      tabItem(tabName = "tabGlobal"),
      #Monetization - Posting Flow - Hide Description Field
      tabItem(tabName = "tabFY2018AdsRanking",
              module_CARS5999_UI("AdsRanking"))
    )
  )
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { 
callModule(module_CARS5999, "AdsRanking")
}
# Run the application 
shinyApp(ui = ui, server = server)

