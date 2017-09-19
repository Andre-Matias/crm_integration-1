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
                box(
                  plotOutput("DropPostingFlowReasons", height = 250)),
                box(
                  checkboxGroupInput("inputDropPostingFlowReasons", "Reasons:", 
                                     choices = unique(dfAll$reason),
                                     selected = unique(dfAll$reason) 
                                     )
                  ),
                box(
                  checkboxGroupInput("inputDropPostingFlowProject", "Project:", 
                                     choices = unique(dfAll$project),
                                     selected = unique(dfAll$project) 
                  )
                )
                )
              )
    )
  )
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { 

  dfDropPostingFlowReasons <- 
    reactive({
    test <-       
      dfAll %>%
      filter(project %in% input$inputDropPostingFlowProject,
             reason %in% input$inputDropPostingFlowReasons,
             event != "posting_leaving_reason_show") %>%
      group_by(date, reason) %>%
      summarise(qty = sum(qty)) %>%
      mutate(perQty = qty / sum(qty))
    test
  })
  
  
  output$DropPostingFlowReasons <- 
    renderPlot({
      ggplot(dfDropPostingFlowReasons()) +
        geom_line(stat = "identity", 
                  aes(date,perQty, group = reason, colour= reason))+
        scale_y_continuous(labels = percent)+
        theme_fivethirtyeight()+
        theme(legend.position="bottom")+
        guides(col = guide_legend(nrow = 2, bycol = TRUE))+
        ggtitle("Drop Off Ad Posting Funnel Reasons",
                subtitle = paste("Platforms:",
                  paste(input$inputDropPostingFlowProject, collapse = " ")
                  )
                )
    })
  }

# Run the application 
shinyApp(ui = ui, server = server)

