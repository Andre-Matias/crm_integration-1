#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.

library(shiny)
library(dplyr)

# Global
source("ret_lead_cons_function.R")
source("ret_anything.R")


# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("OKRs for Onboarding"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(
         checkboxGroupInput("show_vars", "Weeks to show:", unique(dNewUsers2$week), selected = tail(unique(dNewUsers2$week),5))
         ),
      
      
      
      # Show a plot of the generated distribution
      mainPanel(
         ## 1st okr
         plotOutput("retPlot", height = 500),
         br(),
         DT::dataTableOutput("retTable"),
         
         br(),
         br(),
         ## 2nd okr
         plotOutput("retAnyPlot", height = 500),
         br(),
         DT::dataTableOutput("retAnyTable")
         
      )
   )
)


# Define server logic 
server <- function(input, output) {
    
    # df <- reactive({
    #   df<- filter(dNewUsers2, week %in% input$show_vars)
    #   df
    # })
    

   output$retPlot <- renderPlot({
     dNewUsers2<- filter(dNewUsers2, week %in% input$show_vars)
      ggplot(data = dNewUsers2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
       scale_y_continuous(labels = scales::percent, breaks = seq(0,0.20,0.01), limits = c(0,0.20))+
       scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro")+
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
       scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30)) +ggtitle("New Users That Return and do Anything - Time to Return (days)", subtitle = " otomoto.pl")+
       theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono")) + xlab("days to convert") + ylab("% new users") 
     
   })
   
   output$retAnyTable <- DT::renderDataTable({
     
     ret_any_table <- filter(ret_any_table, week %in% input$show_vars)
     DT::datatable(ret_any_table, options = list(dom = 't'), class = "compact", rownames= FALSE)
   })
   
}

# Run the application 
shinyApp(ui = ui, server = server)

