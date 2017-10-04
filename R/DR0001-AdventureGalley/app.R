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

# load modules ----------------------------------------------------------------
source("Module_GVPI-116.PostingFlowHideNotMandatoryFields.R")
source("Module_GVPI-88.StradiaStockarsMessages.R")

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
                                      tabName = 'tabDropReasons'),
                          menuSubItem('Hide Description Field',
                                      tabName = 'tabHideDescriptionField')
                          )
                 )
      ),
      sidebarMenu(
        menuItem("Business Tool", tabName = "bt", icon = icon("wrench"),
                 menuItem('Messages', tabName = 'tabStockarsMessages',
                          menuSubItem('Sync Time: Stockars/Stradia',
                                      tabName = 'tabSyncTimeStkStr')
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
    
      # Monetization - Posting Flow - Drop Reasons 
      tabItem(tabName = "tabDropReasons",
              fluidRow(
                box(
                  plotOutput("DropPostingFlowReasons", height = 400)),
                box(
                  dateRangeInput("dateSelector", label = "Date Range",
                                 start = max(dfAll$date)-7,
                                 end = max(dfAll$date), 
                                 min = min(dfAll$date),
                                 max = max(dfAll$date), format = "yyyy-mm-dd", 
                                 language = "en", separator = " to ", width = NULL
                                 )
                ),
                box(
                  checkboxGroupInput("inputDropPostingFlowReasons", "Reasons:", 
                                     choices =
                                       unique(
                                         dfAll$reason[!is.na(dfAll$reason)]),
                                     selected =
                                       unique(
                                         dfAll$reason[!is.na(dfAll$reason)]) 
                                     )
                  ),
                box(
                  checkboxGroupInput("inputDropPostingFlowProject", "Project:", 
                                     choices =
                                       unique(
                                         dfAll$project[!is.na(dfAll$project)]),
                                     selected =
                                       unique(
                                         dfAll$project[!is.na(dfAll$project)]) 
                  )
                ),
                box(
                  checkboxGroupInput("inputDropPostingFlowPlatform", "Platform:", 
                                     choices =
                                       unique(
                                         dfAll$platform[!is.na(dfAll$platform)]),
                                     selected =
                                       unique(
                                         dfAll$platform[!is.na(dfAll$platform)]) 
                  )
                )
                )
              ),
      # Monetization - Posting Flow - Hide Description Field
      tabItem(tabName = "tabHideDescriptionField",
                module_GVPI116_UI("HideDescription")
              ),
      # Business Tool - Messages - Sync Time Stockars/Stradia
      tabItem(tabName = "tabSyncTimeStkStr",
              module_GVPI88_UI("MessageSyncTime")
      )
    )
  )
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { 

# Monetization - Posting Flow - Drop Reasons - START --------------------------
  
  dfDropPostingFlowReasons <- 
    reactive({
    test <-       
      dfAll %>%
      filter(project %in% input$inputDropPostingFlowProject,
             reason %in% input$inputDropPostingFlowReasons,
             platform %in% input$inputDropPostingFlowPlatform,
             event != "posting_leaving_reason_show",
             date >= ifelse(is.na(max(input$dateSelector)),
                            max(dfAll$date) - 7,
                            min(input$dateSelector)),
             date <= ifelse(is.na(max(input$dateSelector)), 
                            max(dfAll$date) + 1,
                            max(input$dateSelector))
             )%>%
      group_by(date, reason) %>%
      summarise(qty = sum(qty)) %>%
      mutate(perQty = qty / sum(qty))
    test
  })
  
  
  output$DropPostingFlowReasons <- 
    renderPlot({
      ggplot(dfDropPostingFlowReasons()) +
        geom_line(stat = "identity", 
                  aes(date, perQty, group = reason, colour= reason))+
        geom_text(aes(date, perQty, group = reason,
                      label= percent(perQty), colour= reason), vjust = 0)+
        scale_y_continuous(labels = percent, limits = c(0,1))+
        theme_fivethirtyeight()+
        theme(legend.position="bottom")+
        guides(col = guide_legend(nrow = 2, bycol = TRUE))+
        ggtitle("Drop Off Ad Posting Funnel Reasons",
                subtitle = paste(
                  "Project:",
                  paste(input$inputDropPostingFlowProject, collapse = " "),
                  "| Platform: ",
                  paste(input$inputDropPostingFlowPlatform, collapse = " ")
                  )
                )
    })
# Monetization - Posting Flow - Drop Reasons - END --------------------------

# Monetization - Posting Flow - Hide Description - START ----------------------

callModule(module_GVPI116, "HideDescription")

# Monetization - Posting Flow - Hide Description - END ------------------------
  
  
# Business Tool - Messages - Sync Time Stockars/Stradia - START ---------------
  
callModule(module_GVPI88, "MessageSyncTime")

# Business Tool - Messages - Sync Time Stockars/Stradia - END -----------------
}

# Run the application 
shinyApp(ui = ui, server = server)

