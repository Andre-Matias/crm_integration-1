# Load up the shiny package.
library(shiny)

library(shinyBS)

library("ggplot2")

library(directlabels)

library(data.table)

#Load the file containing the Data from GoogleAnalytics
load("ExibitionStandVirtual.RData")
#Load the file containing the Data from GoogleAnalytics
load("rawDataFromStandVirtual.RData")
#Load the file containing the Data from GoogleAnalytics
load("rawDataFromStandVirtualperDevice.RData")

DataStandVirtual$Date <- as.Date(DataStandVirtual$Date)
rawDataFromStandVirtual$Date <- as.Date(rawDataFromStandVirtual$Date)
rawDataFromStandVirtualperDevice$Date <- as.Date(rawDataFromStandVirtualperDevice$Date)
DataStandVirtual$`Showing Phone` <- as.integer(DataStandVirtual$`Showing Phone`)


#Define the Shiny Server informations
server <- function(input, output, session) {

  output$StandVirtualSession <- renderPlot({
    FilteredDataSet <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                          DataStandVirtual$Date <= input$date_range[2]&
                                          DataStandVirtual$Segment == input$SegmentTraffic,]
    FilteredDataSet$Sessions <- as.numeric(FilteredDataSet$Sessions)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Sessions,
               group=FilteredDataSet$Segment,
               colour=FilteredDataSet$Segment,
               label=FilteredDataSet$Sessions))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Sessions")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device")  +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
    
    
  }
  )

  
  output$StandPageView <- renderPlot({
    FilteredDataSet <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                          DataStandVirtual$Date <= input$date_range[2]&
                                          DataStandVirtual$Segment == input$SegmentTraffic,]
    
    FilteredDataSet$'Page View' <- as.numeric(FilteredDataSet$'Page View')
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Page View',
               group=FilteredDataSet$Segment,
               colour=FilteredDataSet$Segment,
               label=FilteredDataSet$'Page View'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Page View")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device")  +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualUniqueUsers <- renderPlot({
    FilteredDataSet <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                          DataStandVirtual$Date <= input$date_range[2]&
                                          DataStandVirtual$Segment == input$SegmentTraffic,]
    
    
    FilteredDataSet$Users <- as.numeric(FilteredDataSet$Users)
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$Users,
               group=FilteredDataSet$Segment,
               colour=FilteredDataSet$Segment,
               label=FilteredDataSet$Users))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14),plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Unique Users")+ 
      scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device")  +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualBounceRate <- renderPlot({
    FilteredDataSet <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                          DataStandVirtual$Date <= input$date_range[2]&
                                          DataStandVirtual$Segment == input$SegmentTraffic,]
    
    FilteredDataSet$'Bounce Rate' <- as.numeric(FilteredDataSet$'Bounce Rate')
    FilteredDataSet$'Bounce Rate' <- format(round(FilteredDataSet$'Bounce Rate', 2), nsmall = 2)
    
    ggplot(FilteredDataSet, 
           aes(FilteredDataSet$Date, 
               FilteredDataSet$'Bounce Rate',
               group=FilteredDataSet$Segment,
               colour=FilteredDataSet$Segment,
               label=FilteredDataSet$'Bounce Rate'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Bounce Rate")+ 
      #scale_y_continuous(labels = scales::comma)+
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualShowPhone <- renderPlot({
    FilteredDataSetPerDevice <- DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                                   DataStandVirtual$Date <= input$date_range[2]&
                                                   DataStandVirtual$Segment == input$SegmentContact,]
    
    
    ggplot(FilteredDataSetPerDevice, 
           aes(FilteredDataSetPerDevice$Date, 
               FilteredDataSetPerDevice$'Showing Phone',
               group=FilteredDataSetPerDevice$Segment,
               colour=FilteredDataSetPerDevice$Segment,
               label=FilteredDataSetPerDevice$'Showing Phone'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Showing Phone")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualMessages <- renderPlot({
    FilteredDataSetPerDevice <- rawDataFromStandVirtualperDevice[rawDataFromStandVirtualperDevice$Date >= input$date_range[1] & 
                                                                   rawDataFromStandVirtualperDevice$Date <= input$date_range[2]&
                                                                   rawDataFromStandVirtualperDevice$Segment == input$SegmentContact,]
    
    
    ggplot(FilteredDataSetPerDevice, 
           aes(FilteredDataSetPerDevice$Date, 
               FilteredDataSetPerDevice$'Replies - Messages',
               group=FilteredDataSetPerDevice$Segment,
               colour=FilteredDataSetPerDevice$Segment,
               label=FilteredDataSetPerDevice$'Replies - Messages'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Replies - Messages")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualMessagesCar <- renderPlot({
    FilteredDataSetPerDevice <- rawDataFromStandVirtualperDevice[rawDataFromStandVirtualperDevice$Date >= input$date_range[1] & 
                                                                   rawDataFromStandVirtualperDevice$Date <= input$date_range[2]&
                                                                   rawDataFromStandVirtualperDevice$Segment == input$SegmentContact,]
    
    
    ggplot(FilteredDataSetPerDevice, 
           aes(FilteredDataSetPerDevice$Date, 
               FilteredDataSetPerDevice$'Replies - Messages Cars',
               group=FilteredDataSetPerDevice$Segment,
               colour=FilteredDataSetPerDevice$Segment,
               label=FilteredDataSetPerDevice$'Replies - Messages Cars'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Replies - Messages Cars")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualRepliers <- renderPlot({
    FilteredDataSetPerDevice <- rawDataFromStandVirtualperDevice[rawDataFromStandVirtualperDevice$Date >= input$date_range[1] & 
                                                                   rawDataFromStandVirtualperDevice$Date <= input$date_range[2]&
                                                                   rawDataFromStandVirtualperDevice$Segment == input$SegmentContact,]
    
    
    ggplot(FilteredDataSetPerDevice, 
           aes(FilteredDataSetPerDevice$Date, 
               FilteredDataSetPerDevice$'Repliers',
               group=FilteredDataSetPerDevice$Segment,
               colour=FilteredDataSetPerDevice$Segment,
               label=FilteredDataSetPerDevice$'Repliers'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Repliers")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualRevListingsPrivate <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    FilteredDataSetRaw$'Rev. Listings Privates' <- as.numeric(FilteredDataSetRaw$'Rev. Listings Privates')
    FilteredDataSetRaw$'Rev. Listings Privates' <- format(round(FilteredDataSetRaw$'Rev. Listings Privates', 2), nsmall = 2)
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Rev. Listings Privates',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Rev. Listings Privates'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Rev. Listings Privates")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualRevListingsDealers <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    FilteredDataSetRaw$'Rev. Listings Dealers' <- as.numeric(FilteredDataSetRaw$'Rev. Listings Dealers')
    FilteredDataSetRaw$'Rev. Listings Dealers' <- format(round(FilteredDataSetRaw$'Rev. Listings Dealers', 2), nsmall = 2)
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Rev. Listings Dealers',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Rev. Listings Dealers'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Rev. Listings Dealers")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )  
  
  output$StandVirtualRevVasPrivate <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    FilteredDataSetRaw$'Rev. VAS Private' <- as.numeric(FilteredDataSetRaw$'Rev. VAS Private')
    FilteredDataSetRaw$'Rev. VAS Private' <- format(round(FilteredDataSetRaw$'Rev. VAS Private', 2), nsmall = 2)
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Rev. VAS Private',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Rev. VAS Private'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Rev. VAS Private")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )  
  
  output$StandVirtualRevVasDealers <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    FilteredDataSetRaw$'Rev. VAS Dealers' <- as.numeric(FilteredDataSetRaw$'Rev. VAS Dealers')
    FilteredDataSetRaw$'Rev. VAS Dealers' <- format(round(FilteredDataSetRaw$'Rev. VAS Dealers', 2), nsmall = 2)
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Rev. VAS Dealers',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Rev. VAS Dealers'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Rev. VAS Dealers")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  ) 
  
  output$StandVirtualRevExportToOlx <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    FilteredDataSetRaw$'Rev. Export to OLX' <- as.numeric(FilteredDataSetRaw$'Rev. Export to OLX')
    FilteredDataSetRaw$'Rev. Export to OLX' <- format(round(FilteredDataSetRaw$'Rev. Export to OLX', 2), nsmall = 2)
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Rev. Export to OLX',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Rev. Export to OLX'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Rev. Export to OLX")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  ) 
  
  output$StandVirtualNNLPrivateGeneral <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'NNLs Privates - General',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'NNLs Privates - General'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="NNLs Privates - General")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  ) 
  
  output$StandVirtualNNLDealersGeneral <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'NNLs Dealers - General',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'NNLs Dealers - General'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="NNLs Dealers - General")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  ) 
  
  output$StandVirtualNNLDealersCar <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'NNLs Dealers - Cars',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'NNLs Dealers - Cars'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="NNLs Dealers - Cars")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  ) 
  
  output$StandVirtualRenewalPrivateGeneral <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Renewals Privates - General',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Renewals Privates - General'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Renewals Privates - General")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualRenewalDealersGeneral <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Renewals Dealers - General',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Renewals Dealers - General'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Renewals Dealers - General")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualRenewalDealersCar <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Renewals Dealers - Car',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Renewals Dealers - Car'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Renewals Dealers - Car")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$StandVirtualNPackagesBought <- renderPlot({
    FilteredDataSetRaw <- rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                                    rawDataFromStandVirtual$Date <= input$date_range[2],]
    
    
    ggplot(FilteredDataSetRaw, 
           aes(FilteredDataSetRaw$Date, 
               FilteredDataSetRaw$'Number of Packages bought',
               group=FilteredDataSetRaw$Segment,
               colour=FilteredDataSetRaw$Segment,
               label=FilteredDataSetRaw$'Number of Packages bought'))+
      geom_line(stat = "identity")+
      theme(text = element_text(size=14), plot.title = element_text(hjust = 0.5)) +
      labs(x="Date",y="Number of Packages bought")+ 
      scale_colour_discrete(name  ="Device") +
      geom_vline(xintercept = as.numeric(as.Date("2017-02-20")))+ 
      geom_text(check_overlap = TRUE, colour = "black")
    
  }
  )
  
  output$downloadTrafficData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(DataStandVirtual[DataStandVirtual$Date >= input$date_range[1] & 
                                   DataStandVirtual$Date <= input$date_range[2]&
                                   DataStandVirtual$Segment == input$SegmentTraffic,], file)
    }
  )
  output$downloadRepliesData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(rawDataFromStandVirtualperDevice[rawDataFromStandVirtualperDevice$Date >= input$date_range[1] & 
                                                   rawDataFromStandVirtualperDevice$Date <= input$date_range[2]&
                                                   rawDataFromStandVirtualperDevice$Segment == input$SegmentContact,], file)
    }
  )  
  output$downloadRevenueData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                          rawDataFromStandVirtual$Date <= input$date_range[2],], file)
    }
  )
  
  output$downloadNNLData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                          rawDataFromStandVirtual$Date <= input$date_range[2],], file)
    }
  )
  
  output$downloadRenewalData <- downloadHandler(
    filename = function() { paste(input$date_range[1], '.csv', sep='') },
    content = function(file) {
      write.csv(rawDataFromStandVirtual[rawDataFromStandVirtual$Date >= input$date_range[1] & 
                                          rawDataFromStandVirtual$Date <= input$date_range[2],], file)
    }
  )
  
}

#Define the UI Shiny Informations
ui <- fluidPage(
  #Title of the page
  titlePanel("StandVirtual Migration KPI - 15 Minutes"),
  helpText("Dashboard to compare the KPI before and after the migration."),
  sidebarPanel(
    sliderInput("date_range", "Choose Date Range:", min = min(DataStandVirtual$Date[DataStandVirtual$Source == "ATI"]),
                max = max(DataStandVirtual$Date[DataStandVirtual$Source == "ATI"], 1),
                value = c(max(DataStandVirtual$Date[DataStandVirtual$Source == "ATI"])-90,max(DataStandVirtual$Date[DataStandVirtual$Source == "ATI"])),
                timeFormat = "%Y-%m-%d", ticks = F, animate = F,width = '98%'),
    
    hr(),
    helpText("Source: ATI and Database"),
    h6("Author: Rodrigo de Caro"),
    width = 2),
  
  mainPanel(
    tabsetPanel(id = "tabSelected",
                tabPanel("Traffic", 
                         div(style="display:inline-block;", selectInput("SegmentTraffic", "Device:", 
                                     choices=list("DESKTOP","RWD","ANDROID","IOS","ALL"),
                                     selected = 1), style="float:left"),
                         div(style="display:inline-block;", downloadButton('downloadTrafficData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         br(),
                         br(),
                         br(),
                         plotOutput("StandVirtualSession"),
                         plotOutput("StandPageView"),
                         plotOutput("StandVirtualUniqueUsers"),
                         plotOutput("StandVirtualBounceRate")
                         
                ),
                tabPanel("Replies", 
                         div(style="display:inline-block;", selectInput("SegmentContact", "Device:", 
                                     choices=list("DESKTOP","RWD","ANDROID","IOS","From OLX","ALL"),
                                     selected = 1), style="float:left"),
                         div(style="display:inline-block;", downloadButton('downloadRepliesData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         br(),
                         br(),
                         br(),
                         plotOutput("StandVirtualShowPhone"),
                         plotOutput("StandVirtualMessages"),
                         plotOutput("StandVirtualMessagesCar"),
                         plotOutput("StandVirtualRepliers")
                         
                ),
                tabPanel("Revenue",
                         br(), 
                         div(style="display:inline-block;", downloadButton('downloadRevenueData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         plotOutput("StandVirtualRevListingsPrivate"),
                         plotOutput("StandVirtualRevListingsDealers"),
                         plotOutput("StandVirtualRevVasPrivate"),
                         plotOutput("StandVirtualRevVasDealers"),
                         plotOutput("StandVirtualRevExportToOlx")
                         
                         
                ),
                tabPanel("NNL", 
                         br(),
                         div(style="display:inline-block;", downloadButton('downloadNNLData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         plotOutput("StandVirtualNNLPrivateGeneral"),
                         h6("Parts are excluded to 2017-02-20"),
                         plotOutput("StandVirtualNNLDealersGeneral"),
                         plotOutput("StandVirtualNNLDealersCar")
                         
                ),
                tabPanel("Renewal", 
                         br(),
                         div(style="display:inline-block;", downloadButton('downloadRenewalData', 'Download'), style="float:right"),
                         br(),
                         br(),
                         plotOutput("StandVirtualRenewalPrivateGeneral"),
                         plotOutput("StandVirtualRenewalDealersGeneral"),
                         plotOutput("StandVirtualRenewalDealersCar"),
                         plotOutput("StandVirtualNPackagesBought")
                         
                )
                
    ),width = 10
  ))


shinyApp(ui = ui, server = server)

