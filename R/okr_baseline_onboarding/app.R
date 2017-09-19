#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.

library(shiny)

# Global
source("ret_lead_cons_function.R")

# Define UI for application that draws a histogram
ui <- fluidPage(
   
   # Application title
   titlePanel("OKR's for Onboarding"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(
         checkboxGroupInput("show_vars", "Weeks to show:", unique(dNewUsers2$week), selected = unique(dNewUsers2$week))
         ),
      
      
      
      # Show a plot of the generated distribution
      mainPanel(
         # plotOutput("distPlot"),
         plotOutput("retPlot")
      )
   )
)

# Define server logic 
server <- function(input, output) {
    
    df <- reactive({
      df<- filter(dNewUsers2, week %in% input$show_vars)
      df
    })
  
  
   
   output$retPlot <- renderPlot({
      ggplot(data = df()) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
       scale_y_continuous(labels = scales::percent, breaks = seq(0,0.20,0.01), limits = c(0,0.20))+
       scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro")+
       theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 
    
   })
   
   
}

# Run the application 
shinyApp(ui = ui, server = server)

