# load libraries --------------------------------------------------------------
library("shiny")
library("shinydashboard")
library("aws.s3")
library("ggplot2")
library("magrittr")
library("data.table")
library("dplyr")
library("dtplyr")


# load mixpanel user's credentials --------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

dfAll <- 
  s3readRDS(object = "PostingFlowDropReasons.RDS", 
            bucket = "pyrates-data-ocean/GVPI-112"
  )

ui <- 
  dashboardPage(
  dashboardHeader(title = "Adventure Galley"),

# dashboardSidebar start ------------------------------------------------------

  dashboardSidebar(
      sidebarMenu(
        menuItem("Global", tabName = 'tabGlobal', icon = icon('dashboard')),
        menuItem("Monetization", tabName = 'tabMonetization', icon = icon('money'),
                 menuItem('Posting Flow', tabName = 'tabPostingFlow', 
                          menuSubItem('Drop Reasons', tabName = 'tabDropReasons'))
                 )
      ),
      sidebarMenu(
        menuItem("Business Tool", tabName = "bt", icon = icon("wrench"))
      )
  ),
# dashboardSidebar end --------------------------------------------------------

# dashboardbody start ---------------------------------------------------------
  dashboardBody(
    tabItems(
      
      # Global 
      tabItem(tabName = "tabGlobal"),
    
      # Monetization - Posting Flow - Drop Reasons 
      tabItem(tabName = "tabDropReasons",
              fluidRow(
                box(plotOutput("d", height = 250))
              )
              )
    )
  )
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { 
  
  output$d <- 
    renderPlot({
      dfAll %>%
      filter(project == "Otomoto.PL", event == "posting_leaving_reason") %>%
      group_by(date, reason) %>%
      summarise(qty = sum(qty)) %>%
      mutate(perQty = qty / sum(qty)) %>%
      ggplot() +
      geom_line(stat = "identity", 
                aes(date,perQty, group = reason, colour= reason))
    })
  
  }

# Run the application 
shinyApp(ui = ui, server = server)

