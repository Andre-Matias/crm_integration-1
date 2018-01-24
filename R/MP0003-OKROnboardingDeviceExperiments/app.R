###################################################################################################
# OKRs Dashboard to track the new ONBOARDING                                                      #
# New app by countries and platforms                                                              #
# This is a Shiny web application. You can run it by clicking the 'Run App' button above.         #
###################################################################################################

# Global
library(shiny)
library(dplyr)
library(ggplot2)
library(ggthemes)

source("functions.R")
load("ret_files.RData")



# Define UI  -----------------------------------------------------------------------------------
ui <- fluidPage(
   
   # Application title
   titlePanel("OKRs for Onboarding"),
   
   # Sidebar 
   sidebarLayout(
      sidebarPanel(
         checkboxGroupInput("show_vars", "Cohorts to show:", as.character(unique(ret_any_pl$week)), 
                            selected = tail(unique(ret_any_pl$week), 2)),
         selectInput("platform_filter", "Platform:", c("responsive"="rwd",
                                                   "desktop"="desktop",
                                                   "ios"="ios",
                                                   "android"="android")),
         # helpText("Each line shows the daily retention rate for a specific cohort of new users. 
         # Example: for the cohort of new users adcquired in the week starting on the 4th of September, 11% 
         # came back after 1 day, 7% after 2 days, etc. and do something."),

         helpText("OKR 1: Increase % of new users returning after 7 days by 10%"),
         helpText("OKR 2: Increase % of new users making a lead after 14 days by 10%"),
         br(),
         helpText(paste("Last update:", "", last_update)),
         br(),
         helpText( "Other", 
                   a("Healthy Metrics", href="https://triton.olxgroup.bi/#/site/europe/views/HealthyMetrics-onboarding/HealthyMetricsOnboarding", target="_blank")
         )

         ),
      
      
   # Main Panel with visualizations   
      mainPanel(
        
        tabsetPanel(type = "tabs",
          
          tabPanel("Otomoto PL",
             br(),
             plotOutput("ret_any_pl", height = 400),
             br(),
             #helpText("OKR 1: Increase % of new users returning after 1 week by 10%"),
             DT::dataTableOutput("ret_any_table_pl"),
             br(),
             plotOutput("comp_okr1_pl", height = 400),
             br(),
             br(),
             br(),
             plotOutput("ret_lead_pl", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_pl"),
             br(),
             plotOutput("comp_okr2_pl", height = 400),
             br()
                  ),
          
          tabPanel("Standvirtual PT",
             br(), 
             plotOutput("ret_any_pt", height = 400),
             br(),
             DT::dataTableOutput("ret_any_table_pt"),
             br(),
             plotOutput("comp_okr1_pt", height = 400),
             br(),
             br(),
             br(),
             
             plotOutput("ret_lead_pt", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_pt"),
             br(),
             plotOutput("comp_okr2_pt", height = 400),
             br()
                  ),
          
          tabPanel("Autovit RO",
             br(),
             plotOutput("ret_any_ro", height = 400),
             br(),
             DT::dataTableOutput("ret_any_table_ro"),
             br(),
             plotOutput("comp_okr1_ro", height = 400),
             br(),
             br(),
             br(),
             
             plotOutput("ret_lead_ro", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_ro"),
             br(),
             plotOutput("comp_okr2_ro", height = 400),
             br()
                  )
          
        )
      )
   )
)



# Define server logic -----------------------------------------------------------------------------
server <- function(input, output) {

   
   ##### Otomoto PL tab --------------
   output$ret_any_pl <- renderPlot({
     ret_any_pl <- filter(ret_any_pl, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     # use retPlot() function instead of ggplot chunk
     # max= max(ret_any_pl$ret, na.rm = T)+0.02
     retPlot(ret_any_pl, title="OKR1: baseline for original", 
             subtitle="% of new users that return each day")
   })
   
   output$ret_any_table_pl <- DT::renderDataTable({
    ret_any_table_pl <- filter(ret_any_table_pl, week %in% input$show_vars & platform==input$platform_filter)
    DT::datatable(ret_any_table_pl, options = list(dom = 't'), class = "compact", rownames= FALSE
                  #caption = 'OKR1: original vs variation. % new users that return each day.'
                  # caption= htmltools::tags$caption(
                  #   style = 'caption-side: right; text-align: left; color:black; 
                  #   font-size:100% ;', tags$b("FRFRFR"))
                  )
   })
   
   output$comp_okr1_pl <- renderPlot({
     comp_okr1_pl <- filter(comp_okr1_pl, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr1_pl, title="OKR1: original vs variation", 
                 subtitle="avg. % of new users that return within 7 days")
   })
   
   
   
   output$ret_lead_pl <- renderPlot({
     ret_lead_pl <- filter(ret_lead_pl, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     retPlot(ret_lead_pl, title="OKR2: baseline for original", 
             subtitle="% of new users that make a lead each day")
   })
   
   output$ret_lead_table_pl <- DT::renderDataTable({
     ret_lead_table_pl <- filter(ret_lead_table_pl, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_pl, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$comp_okr2_pl <- renderPlot({
     comp_okr2_pl <- filter(comp_okr2_pl, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr2_pl, title="OKR2: original vs variation", 
                 subtitle="avg. % of new users that make a lead within 14 days")
   })
   
   
   ##### Standvirtual PT tab ----------
   output$ret_any_pt <- renderPlot({
     ret_any_pt <- filter(ret_any_pt, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     retPlot(ret_any_pt, title="OKR1: baseline for original", 
             subtitle="% of new users that return each day")
   })
   
   output$ret_any_table_pt <- DT::renderDataTable({
     ret_any_table_pt <- filter(ret_any_table_pt, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_any_table_pt, options = list(dom = 't'), class = "compact", rownames= FALSE

     )
   })
   
   output$comp_okr1_pt <- renderPlot({
     comp_okr1_pt <- filter(comp_okr1_pt, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr1_pt, title="OKR1: original vs variation", 
                 subtitle="avg. % of new users that return within 7 days")
   })
   
   
   output$ret_lead_pt <- renderPlot({
     ret_lead_pt <- filter(ret_lead_pt, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     retPlot(ret_lead_pt, title="OKR2: baseline for original", 
             subtitle="% of new users that make a lead each day")
   })
   
   output$ret_lead_table_pt <- DT::renderDataTable({
     ret_lead_table_pt <- filter(ret_lead_table_pt, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_pt, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$comp_okr2_pt <- renderPlot({
     comp_okr2_pt <- filter(comp_okr2_pt, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr2_pt, title="OKR2: original vs variation", 
                 subtitle="avg. % of new users that make a lead within 14 days")
   })
   
   
   ##### Autovit RO tab ---------------

   output$ret_any_ro <- renderPlot({
     ret_any_ro <- filter(ret_any_ro, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     retPlot(ret_any_ro, title="OKR1: baseline for original", 
             subtitle="% of new users that return each day")
   })
   
   output$ret_any_table_ro <- DT::renderDataTable({
     ret_any_table_ro <- filter(ret_any_table_ro, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_any_table_ro, options = list(dom = 't'), class = "compact", rownames= FALSE
                   
     )
   })
   
   output$comp_okr1_ro <- renderPlot({
     comp_okr1_ro <- filter(comp_okr1_ro, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr1_ro, title="OKR1: original vs variation", 
                 subtitle="avg. % of new users that return within 7 days")
   })

   
   
   output$ret_lead_ro <- renderPlot({
     ret_lead_ro <- filter(ret_lead_ro, week %in% input$show_vars & platform==input$platform_filter & version=="original A")
     retPlot(ret_lead_ro, title="OKR2: baseline for original", 
             subtitle="% of new users that make a lead each day")
   })
   
   output$ret_lead_table_ro <- DT::renderDataTable({
     ret_lead_table_ro <- filter(ret_lead_table_ro, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_ro, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$comp_okr2_ro <- renderPlot({
     comp_okr2_ro <- filter(comp_okr2_ro, week %in% input$show_vars & platform==input$platform_filter)
     retCompPlot(comp_okr2_ro, title="OKR2: original vs variation", 
                 subtitle="avg. % of new users that make a lead within 14 days")
   })
   
   
}

  

# Run the application 
shinyApp(ui = ui, server = server)

