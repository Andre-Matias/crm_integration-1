# Load up the shiny package.
library(shiny)

library(shinyBS)

library("ggplot2")

library(directlabels)

library(data.table)

#Load the file containing the Data from GoogleAnalytics
load("ExibitionTradus.RData")


#Define the Shiny Server informations
server <- function(input, output, session) {
  
  
  output$TradusDAU <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                            DataTradus$Date <= input$date_range[2]&
                                            DataTradus$Device == input$deviceTraffic,]
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Users,
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$Users))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Daily Active Users")+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      scale_colour_discrete(name  ="Device")+
      geom_hline(yintercept = mean(FilteredDataSet$Users), color="blue") 
    
    
    
  }
  )

  output$TradusSession <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                            DataTradus$Date <= input$date_range[2]&
                                            DataTradus$Device == input$deviceTraffic,]
    
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Sessions,
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$Sessions))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Sessions")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device") +
      #geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      geom_hline(yintercept = mean(FilteredDataSet$Sessions), color="blue")
    
    
    
  }
  )

  output$TradusPageView <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                            DataTradus$Date <= input$date_range[2]&
                                            DataTradus$Device == input$deviceTraffic,]
    
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Page View',
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$'Page View'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Page View")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device") +
      #geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      geom_hline(yintercept = mean(FilteredDataSet$'Page View'), color="blue")
    
  }
  )
  
  output$TradusUniqueUsers <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                          DataTradus$Date <= input$date_range[2]&
                                          DataTradus$Device == input$deviceTraffic,]
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Users,
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$Users))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Unique Users")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device") +
      #geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      geom_hline(yintercept = mean(FilteredDataSet$Users), color="blue")
    
  }
  )
  
  output$TradusBounceRate <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                          DataTradus$Date <= input$date_range[2]&
                                          DataTradus$Device == input$deviceTraffic,]
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    
    FilteredDataSet$'Bounce Rate' <- as.numeric(FilteredDataSet$'Bounce Rate')
    FilteredDataSet$'Bounce Rate' <- format(round(FilteredDataSet$'Bounce Rate', 2), nsmall = 2)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Bounce Rate',
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$'Bounce Rate'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Bounce Rate")+ 
      #scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device")+
      #geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      geom_hline(yintercept = mean(FilteredDataSet$'Bounce Rate'), color="blue")
    
  }
  )
  
  output$TradusEnteringVisits <- renderPlot({
    FilteredDataSet <- DataTradus[DataTradus$Date >= input$date_range[1] & 
                                            DataTradus$Date <= input$date_range[2]&
                                            DataTradus$Device == input$deviceTraffic,]
    
    FilteredDataSet$Date <- as.Date(FilteredDataSet$Date)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Entering Visits',
               group=FilteredDataSet$Device,
               colour=FilteredDataSet$Device,
               label=FilteredDataSet$'Entering Visits'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Entering Visits")+ 
      #scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device")+
      #geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")+
      geom_hline(yintercept = mean(FilteredDataSet$'Entering Visits'), color="blue")
    
  }
  )
  
  output$downloadTrafficData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(DataTradus[DataTradus$Date >= input$date_range[1] & 
                                     DataTradus$Date <= input$date_range[2]&
                                     DataTradus$Device == input$deviceTraffic,], file)
    }
  )
 
  
}

#Define the UI Shiny Informations
ui <- fluidPage(
  #Title of the page
  titlePanel("Heavy Machinery"),
  helpText("Dashboard with the mainly metrics to Tradus Pro."),
  sidebarPanel(
    sliderInput("date_range", "Choose Date Range:", min = min(DataTradus$Date),
                max = max(DataTradus$Date),
                value = c(min(DataTradus$Date),max(DataTradus$Date)),
                timeFormat = "%Y-%m-%d", ticks = F, animate = F,width = '98%'),
    
    hr(),
    helpText("Source: GA and Database"),
    h6("Author: Rodrigo de Caro"), 
    width = 2),
  
  mainPanel(
    tabsetPanel(id = "tabSelected",
                tabPanel("Traffic", 
                         div(style="display:inline-block;", selectInput("deviceTraffic", "Device:", 
                                     choices=list("desktop","mobile","tablet","ALL"),
                                     selected = 1), style="float:left"),
                         div(style="display:inline-block;", downloadButton('downloadTrafficData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         br(),
                         br(),
                         br(),
                         plotOutput("TradusDAU"),
                         plotOutput("TradusSession"),
                         plotOutput("TradusPageView"),
                         plotOutput("TradusBounceRate"),
                         plotOutput("TradusEnteringVisits")
                         
                )
               
                
    ),width = 10
  ))


shinyApp(ui = ui, server = server)

