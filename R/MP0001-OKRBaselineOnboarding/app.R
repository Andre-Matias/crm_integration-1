# OKRs Dashboard to track the new ONBOARDING 
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.

library(shiny)
library(dplyr)

# Global
source("ret_lead_cons_function.R")
source("ret_any_cons.R")


# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("OKRs for Onboarding"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(
         checkboxGroupInput("show_vars", "Weeks to show:", unique(dNewUsers2$week), selected = tail(unique(dNewUsers2$week),3))
         ),
      
      
      
      # Show a plot of the generated distribution
      mainPanel(
         ## 1st okr
         plotOutput("retAnyPlot", height = 400),
         br(),
         DT::dataTableOutput("retAnyTable"),
         
         br(),
         br(),
         
         ## 2nd okr
         plotOutput("retPlotNotCum", height = 400),
         br(),
         DT::dataTableOutput("retTableNotCum"),
        
         br(),
         
         plotOutput("retPlot", height = 400),
         br(),
         DT::dataTableOutput("retTable")
         
      )
   )
)


# Define server logic 
server <- function(input, output) {
    
    # df <- reactive({
    #   df<- filter(dNewUsers2, week %in% input$show_vars)
    #   df
    # })
    
  #2nd okr graph + data table
  
  ## not cumulated
  output$retPlotNotCum <- renderPlot({
    dNewUsers2<- filter(dNewUsers2_not_cum, week %in% input$show_vars)
    ggplot(data = dNewUsers2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
      scale_y_continuous(labels = scales::percent, breaks = seq(0,0.10,0.01), limits = c(0,0.10))+
      scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is not cumulated")+
      theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 
    
  })
  
  
  output$retTableNotCum <- DT::renderDataTable({
    
    ret_table_cons_not_cum <- filter(ret_table_cons_not_cum, week %in% input$show_vars)
    DT::datatable(ret_table_cons_not_cum, options = list(dom = 't'), class = "compact", rownames= FALSE)
  }) 
  
  ## cumulated
  output$retPlot <- renderPlot({
     dNewUsers2<- filter(dNewUsers2, week %in% input$show_vars)
      ggplot(data = dNewUsers2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
       scale_y_continuous(labels = scales::percent, breaks = seq(0,0.20,0.01), limits = c(0,0.20))+
       scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is cumulated")+
       theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 
    
   })
   
   
   output$retTable <- DT::renderDataTable({
     
     ret_table_cons <- filter(ret_table_cons, week %in% input$show_vars)
     DT::datatable(ret_table_cons, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
   
   
   # add 1st okr graph + data table
   output$retAnyPlot <- renderPlot({
     ret_any_2 <- filter(ret_any_2, week %in% input$show_vars)
     ggplot(data = ret_any_2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
       scale_y_continuous(labels = scales::percent, breaks = seq(0,0.13,0.01), limits = c(0,0.13))+
       scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30)) +ggtitle("New Users That Return and do Anything - Time to Return (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is not cumulated")+
       theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono")) + xlab("days to convert") + ylab("% new users") 
     
   })
   
   output$retAnyTable <- DT::renderDataTable({
     
     ret_any_table <- filter(ret_any_table, week %in% input$show_vars)
     DT::datatable(ret_any_table, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
}

# Run the application 
shinyApp(ui = ui, server = server)

