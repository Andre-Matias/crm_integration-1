# Load up the shiny package.
library(shiny)

library("ggplot2")

library(directlabels)

#Load the file containing the Data from GoogleAnalytics
load("ExibitionStandVirtual.RData")
#Load the file containing the Data from GoogleAnalytics
load("ExibitionImoVirtual.RData")

#Define the Shiny Server informations
server <- function(input, output) {
  output$StandVirtualSession <- renderPlot({
    FilteredDataSet <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                       DataStandVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,3]-FilteredDataSetTempGa[,3])/FilteredDataSetTempGa[,3])))
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Sessions,
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source,
               label=FilteredDataSet$Sessions))+
        geom_line(stat = "identity")+
        ggtitle("Sessions by Tracking Source") +
        theme(plot.title = element_text(hjust = 0.5)) +
        labs(x="Date",y="Sessions")+ 
      scale_y_continuous(labels = scales::comma)+
        scale_colour_discrete(name  ="Source") +
        geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,3], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,3], label=FilteredDataSetCalculate[2,2])
    
    }
  )
  
  output$StandVirtualPageView <- renderPlot({
    FilteredDataSet<-DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                       DataStandVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,4]-FilteredDataSetTempGa[,4])/FilteredDataSetTempGa[,4])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Page View',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Page View by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Page View") +
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,4], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,4], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$StandVirtualBounce <- renderPlot({
    FilteredDataSet<-DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                       DataStandVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,5]-FilteredDataSetTempGa[,5])/FilteredDataSetTempGa[,5])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Bounce',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Bounce by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Bounce")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,5], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,5], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$StandVirtualEnteringVisits <- renderPlot({
    FilteredDataSet<-DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                       DataStandVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,6]-FilteredDataSetTempGa[,6])/FilteredDataSetTempGa[,6])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Entering Visits',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Entering Visits by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Entering Visits")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,6], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,6], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$StandVirtualDAU <- renderPlot({
    FilteredDataSet<-DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                       DataStandVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,7]-FilteredDataSetTempGa[,7])/FilteredDataSetTempGa[,3])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Users',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Daily Active Users by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Daily Active Users")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,7], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,7], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  
  output$ImoVirtualSession <- renderPlot({
    FilteredDataSet<-DataImoVirtual[DataImoVirtual$Date >= input$date_range[1] & 
                       DataImoVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,3]-FilteredDataSetTempGa[,3])/FilteredDataSetTempGa[,3])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Sessions,
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Sessions by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Sessions")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,3], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,3], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$ImoVirtualPageView <- renderPlot({
    FilteredDataSet<-DataImoVirtual[DataImoVirtual$Date >= input$date_range[1] & 
                       DataImoVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,4]-FilteredDataSetTempGa[,4])/FilteredDataSetTempGa[,4])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Page View',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Page View by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Page View")+
      scale_y_continuous(labels = scales::comma)+ 
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,4], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,4], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$ImoVirtualBounce <- renderPlot({
    FilteredDataSet<-DataImoVirtual[DataImoVirtual$Date >= input$date_range[1] & 
                       DataImoVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,5]-FilteredDataSetTempGa[,5])/FilteredDataSetTempGa[,5])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Bounce',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Bounce by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Bounce")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,5], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,5], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$ImoVirtualEnteringVisits <- renderPlot({
    FilteredDataSet<-DataImoVirtual[DataImoVirtual$Date >= input$date_range[1] & 
                       DataImoVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,6]-FilteredDataSetTempGa[,6])/FilteredDataSetTempGa[,6])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Entering Visits',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Entering Visits by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Entering Visits")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,6], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,6], label=FilteredDataSetCalculate[2,2])
    
  }
  )
  
  output$ImoVirtualDAU <- renderPlot({
    FilteredDataSet<-DataImoVirtual[DataImoVirtual$Date >= input$date_range[1] & 
                       DataImoVirtual$Date <= input$date_range[2], ]
    
    FilteredDataSetTempAti <- FilteredDataSet[(FilteredDataSet$Source == "ATI"),]
    
    FilteredDataSetTempAti <- FilteredDataSetTempAti[(FilteredDataSetTempAti$Date == min(FilteredDataSetTempAti$Date) |
                                                        FilteredDataSetTempAti$Date == max(FilteredDataSetTempAti$Date)) ,]
    
    FilteredDataSetTempGa <- FilteredDataSet[(FilteredDataSet$Source == "GA"),]
    
    FilteredDataSetTempGa <- FilteredDataSetTempGa[(FilteredDataSetTempGa$Date == min(FilteredDataSetTempGa$Date) |
                                                      FilteredDataSetTempGa$Date == max(FilteredDataSetTempGa$Date)) ,]
    
    FilteredDataSetCalculate <- data.frame(FilteredDataSetTempGa[,1],
                                           sprintf("%.1f %%", 100*((FilteredDataSetTempAti[,7]-FilteredDataSetTempGa[,7])/FilteredDataSetTempGa[,7])))
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Users',
               group=FilteredDataSet$Source,
               colour=FilteredDataSet$Source))+
      geom_line(stat = "identity")+
      ggtitle("Daily Active Users by Tracking Source") +
      theme(plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Daily Active Users")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Source")+
      geom_smooth(method = "lm",size = 1, se = FALSE)+
      annotate("text", x = FilteredDataSetTempAti[1,1], y = FilteredDataSetTempAti[1,7], label=FilteredDataSetCalculate[1,2])+
      annotate("text", x = FilteredDataSetTempAti[2,1], y = FilteredDataSetTempAti[2,7], label=FilteredDataSetCalculate[2,2])
    
  }
  )
}

#Define the UI Shiny Informations
ui <- fluidPage(
  #Title of the page
  titlePanel("Comparison Between ATI and GA"),
  helpText("Analysis to compare the data collected by ATI and Google Analytics."),
  sidebarPanel(
    sliderInput("date_range", "Choose Date Range:", min = min(DataStandVirtual$Date[DataStandVirtual$Source == "GA"]),
                max = max(DataStandVirtual$Date[DataStandVirtual$Source == "GA"], 1),
                value = c(min(DataStandVirtual$Date[DataStandVirtual$Source == "GA"], 1),max(DataStandVirtual$Date[DataStandVirtual$Source == "GA"])),
                timeFormat = "%Y-%m-%d", ticks = F, animate = F,width = '98%'),
    hr(),
    helpText("Source: Google Analytics and ATI"),
    h6("Author: Rodrigo de Caro"),
    width = 2),
  
  mainPanel(
    tabsetPanel(id = "tabSelected",
                tabPanel("StandVirtual", 
                         h6("Date: ", ExecutedDate),
                         plotOutput("StandVirtualSession"),
                         plotOutput("StandVirtualPageView"),
                         plotOutput("StandVirtualBounce"),
                         plotOutput("StandVirtualEnteringVisits"),
                         plotOutput("StandVirtualDAU")
                ), tabPanel("ImoVirtual", 
                            h6("Date: ", ExecutedDate),
                            plotOutput("ImoVirtualSession"),
                            plotOutput("ImoVirtualPageView"),
                            plotOutput("ImoVirtualBounce"),
                            plotOutput("ImoVirtualEnteringVisits"),
                            plotOutput("ImoVirtualDAU")
                )
                
    ),width = 10
  ))


shinyApp(ui = ui, server = server)

