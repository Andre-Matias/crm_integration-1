# New app by countries and platforms
# OKRs Dashboard to track the new ONBOARDING 
# This is a Shiny web application. You can run it by clicking the 'Run App' button above.

library(shiny)
library(dplyr)
library(ggplot2)
library(ggthemes)

source("functions.R")

# Global
# source("ret_lead_cons_function.R")
# source("ret_any_cons.R")

#source("ret_any_cons_D.R")


# Define UI  -----------------------------------------------------------------------------------
ui <- fluidPage(
   
   # Application title
   titlePanel("OKRs for Onboarding"),
   
   # Sidebar 
   sidebarLayout(
      sidebarPanel(
         checkboxGroupInput("show_vars", "Weeks to show:", unique(ret_any_pl$week), selected = tail(unique(ret_any_pl$week),3)),
         selectInput("platform_filter", "Platform:", c("responsive"="rwd",
                                                   "desktop"="desktop",
                                                   "android"="android",
                                                   "ios"="ios"))
         ),
      
      
   # Main Panel with visualizations   
      mainPanel(
        
        tabsetPanel(type = "tabs",
          
          tabPanel("Otomoto PL",
             plotOutput("ret_any_pl", height = 400),
             br(),
             DT::dataTableOutput("ret_any_table_pl"),
             br(),
             br(),
             plotOutput("ret_lead_pl", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_pl")
                  ),
          
          tabPanel("Standvirtual PT",
             plotOutput("ret_any_pt", height = 400),
             br(),
             DT::dataTableOutput("ret_any_table_pt"),
             br(),
             br(),
             plotOutput("ret_lead_pt", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_pt")
                  ),
          
          tabPanel("Autovit RO",
             plotOutput("ret_any_ro", height = 400),
             br(),
             DT::dataTableOutput("ret_any_table_ro"),
             br(),
             br(),
             plotOutput("ret_lead_ro", height = 400),
             br(),
             DT::dataTableOutput("ret_lead_table_ro")
                  ),
          
          tabPanel("Consolidated",
                   br()
                  )
        )
      )
   )
)



# Define server logic -----------------------------------------------------------------------------
server <- function(input, output) {
    
  # ## cumulated
  # output$retPlot <- renderPlot({
  #    dNewUsers2<- filter(dNewUsers2, week %in% input$show_vars)
  #     ggplot(data = dNewUsers2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
  #      scale_y_continuous(labels = scales::percent, breaks = seq(0,0.20,0.01), limits = c(0,0.20))+
  #      scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is cumulated")+
  #      theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 
  #   
  #  })

   
   ##### Otomoto PL tab --------------
   output$ret_any_pl <- renderPlot({
     ret_any_pl <- filter(ret_any_pl, week %in% input$show_vars & platform==input$platform_filter)
     # use retPlot() function instead of ggplot chunk
     # max= max(ret_any_pl$ret, na.rm = T)+0.02
     retPlot(ret_any_pl, title="New Users That Return and do Anything - Time to Return (days)", subtitle="otomoto.pl. Retention % is not cumulated")
   })
   
   output$ret_any_table_pl <- DT::renderDataTable({
    ret_any_table_pl <- filter(ret_any_table_pl, week %in% input$show_vars & platform==input$platform_filter)
    DT::datatable(ret_any_table_pl, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$ret_lead_pl <- renderPlot({
     ret_lead_pl <- filter(ret_lead_pl, week %in% input$show_vars & platform==input$platform_filter)
     retPlot(ret_lead_pl, title="New Users That Send a Lead - Time to Send (days)", subtitle="otomoto.pl. Retention % is not cumulated")
   })
   
   output$ret_lead_table_pl <- DT::renderDataTable({
     ret_lead_table_pl <- filter(ret_lead_table_pl, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_pl, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   ##### Standvirtual PT tab ----------
   output$ret_any_pt <- renderPlot({
     ret_any_pt <- filter(ret_any_pt, week %in% input$show_vars & platform==input$platform_filter)
     retPlot(ret_any_pt, title="New Users That Return and do Anything - Time to Return (days)", subtitle="standvirtual.pt. Retention % is not cumulated")
     
   })
   
   output$ret_any_table_pt <- DT::renderDataTable({
     ret_any_table_pt <- filter(ret_any_table_pt, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_any_table_pt, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$ret_lead_pt <- renderPlot({
     ret_lead_pt <- filter(ret_lead_pt, week %in% input$show_vars & platform==input$platform_filter)
     retPlot(ret_lead_pt, title="New Users That Send a Lead - Time to Send (days)", subtitle="standvirtual.pt. Retention % is not cumulated")
   })
   
   output$ret_lead_table_pt <- DT::renderDataTable({
     ret_lead_table_pt <- filter(ret_lead_table_pt, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_pt, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   ##### Autovit RO tab ---------------
   output$ret_any_ro <- renderPlot({
     ret_any_ro <- filter(ret_any_ro, week %in% input$show_vars & platform==input$platform_filter)
     retPlot(ret_any_ro, title="New Users That Return and do Anything - Time to Return (days)", subtitle="autovit.ro. Retention % is not cumulated")
     
   })
   
   output$ret_any_table_ro <- DT::renderDataTable({
     ret_any_table_ro <- filter(ret_any_table_ro, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_any_table_ro, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   output$ret_lead_ro <- renderPlot({
     ret_lead_ro <- filter(ret_lead_ro, week %in% input$show_vars & platform==input$platform_filter)
     retPlot(ret_lead_ro, title="New Users That Send a Lead - Time to Send (days)", subtitle="autovit.ro. Retention % is not cumulated")
   })
   
   output$ret_lead_table_ro <- DT::renderDataTable({
     ret_lead_table_ro <- filter(ret_lead_table_ro, week %in% input$show_vars & platform==input$platform_filter)
     DT::datatable(ret_lead_table_ro, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
}

# Run the application 
shinyApp(ui = ui, server = server)

