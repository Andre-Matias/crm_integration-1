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

# load mixpanel user's credentials --------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

# load modules ----------------------------------------------------------------
source("Module_CARS-5999.PercentageOfUsersThatSeeAdsRanking.R")

ui <- 
  dashboardPage(
  dashboardHeader(title = "Adventure Galley"),

# dashboardSidebar start ------------------------------------------------------

  dashboardSidebar(
      sidebarMenu(
        menuItem("Global", tabName = 'tabGlobal', icon = icon('dashboard')),
        menuItem("Monetization", tabName = 'tabMonetization', icon = icon('money'),
                 menuItem('Posting Flow', tabName = 'tabPostingFlow', 
                          menuSubItem('Drop Reasons',
                                      tabName = 'tabHideDescriptionField')
                          )
                 )
      ),
      sidebarMenu(
        menuItem("Business Tool", tabName = "bt", icon = icon("wrench"),
                 menuItem('Messages', tabName = 'tabStockarsMessages',
                          menuSubItem('Sync Time: Stockars/Stradia',
                                      tabName = 'tabSyncTimeStkStr'),
                          menuSubItem('Sync Time: Stockars/OLX',
                                      tabName = 'tabSyncTimeStkOLX')
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
      tabItem(tabName = "tabHideDescriptionField",
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

