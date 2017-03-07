<<<<<<< HEAD


library(ggplot2)
library(shiny)
#library(formattable)

server <- function(input, output) {
  
  
  #load date 
  load("dfstoriadup.RData")
  load("Storiadupfinal2.RData")
  load("dfstradia.RData")
  load("Stradiadupfinal3.RData")
  
  
  #Ajust similarity percentage (Shiny has some problems with that)
  
  Storiadupfinal2$similarity <- paste(round(Storiadupfinal2$similarity*100,digits=1),"%",sep="")
  
  Stradiadupfinal3$similarity <- paste(round(Stradiadupfinal3$similarity*100,digits=1),"%",sep="")
  
  #Tables(daily duplicates)
  
  output$ex1 <- renderDataTable(
    Storiadupfinal2, options = list(pageLength = 30)
  )
  
  output$ex2 <- renderDataTable(
   Stradiadupfinal3, options = list(pageLength = 30)
  )
  
  

  #plot (evolution by month)
  
  #Storia Graph 
  
  options(scipen=10000)
  
  output$duplicatesPlot <- renderPlot({
    
    ggplot(df, aes(Date)) + 
      geom_bar(width=.5,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(70000, 350000)) + 
      geom_text(aes(y= Duplicates,label = Duplicates, vjust=-2)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2))
    
    
  })
  
  #Stradia Graph
  output$duplicatesPlot2 <- renderPlot({
    
    ggplot(dfstradia, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 55000)) + 
      geom_text(aes(y= Duplicates,label = Duplicates, vjust=-1)) + 
      geom_text(aes(y= Ads,label = Ads, vjust=2))
    
  })
  
}

ui <- navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Storia.IND and Stradia.ID Duplicated Ads"),
           mainPanel(
             h5("This report provides data about ad duplications for Storia Indonesia and Stradia India based on some variables."),
             br(),
             h5("To find ad duplicates we use the following variables for Storia: 
                Same user id, same city id, same category id, same price and title similarity with at least 70%.
                For Stradia we use the following variables:
                Same user id, same brand, same model, same year, same mileage and description similarity with at least 70%."),
             br(),
             h5("In Storia and Stradia graph tabs, we have a plot with the relation between active ads and ad duplicates, by day. 
                Due performance and memory capacity reasons, we just consider a maximum of 30 days per plot. With that we can have an overall perspective regarding the evolution of duplicates."), 
             br(),
             h5("In Storia and Stradia tables you can find the current duplications (daily active) and you can look deep for the data using the variables that we used."),
             br(),
             h5("We need to consider that some users can cheat us with different prices, titles, description, mileages or even year so is expected that we might have more duplicates than this report shows."),
             br(), 
             br(),
             h6("Date: 30 days for graphs, current day for tables"),
             h6("Source: Database"),
             h6("Author: Pedro Matos"))),  
  tabPanel('Storia Graph', plotOutput("duplicatesPlot")),   
  tabPanel('Storia Table', dataTableOutput('ex1')),
  tabPanel('Stradia Graph', plotOutput("duplicatesPlot2")),   
  tabPanel('Stradia Table', dataTableOutput('ex2'))
  
)

shinyApp(ui = ui, server = server)








=======


library(ggplot2)
library(shiny)
#library(formattable)

server <- function(input, output) {
  
  
  #load date 
  load("dfstoriadup.RData")
  load("Storiadupfinal2.RData")
  load("dfstradia.RData")
  load("Stradiadupfinal3.RData")
  
  
  #Ajust similarity percentage (Shiny has some problems with that)
  
  Storiadupfinal2$similarity <- paste(round(Storiadupfinal2$similarity*100,digits=1),"%",sep="")
  
  Stradiadupfinal3$similarity <- paste(round(Stradiadupfinal3$similarity*100,digits=1),"%",sep="")
  
  #Tables(daily duplicates)
  
  output$ex1 <- renderDataTable(
    Storiadupfinal2, options = list(pageLength = 30)
  )
  
  output$ex2 <- renderDataTable(
   Stradiadupfinal3, options = list(pageLength = 30)
  )
  
  

  #plot (evolution by month)
  
  #Storia Graph 
  
  options(scipen=10000)
  
  output$duplicatesPlot <- renderPlot({
    
    ggplot(df, aes(Date)) + 
      geom_bar(width=.5,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(70000, 350000)) + 
      geom_text(aes(y= Duplicates,label = Duplicates, vjust=-2)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2))
    
    
  })
  
  #Stradia Graph
  output$duplicatesPlot2 <- renderPlot({
    
    ggplot(dfstradia, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 55000)) + 
      geom_text(aes(y= Duplicates,label = Duplicates, vjust=-1)) + 
      geom_text(aes(y= Ads,label = Ads, vjust=2))
    
  })
  
}

ui <- navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Storia.IND and Stradia.ID Duplicated Ads"),
           mainPanel(
             h5("This report provides data about ad duplications for Storia Indonesia and Stradia India based on some variables."),
             br(),
             h5("To find ad duplicates we use the following variables for Storia: 
                Same user id, same city id, same category id, same price and title similarity with at least 70%.
                For Stradia we use the following variables:
                Same user id, same brand, same model, same year, same mileage and description similarity with at least 70%."),
             br(),
             h5("In Storia and Stradia graph tabs, we have a plot with the relation between active ads and ad duplicates, by day. 
                Due performance and memory capacity reasons, we just consider a maximum of 30 days per plot. With that we can have an overall perspective regarding the evolution of duplicates."), 
             br(),
             h5("In Storia and Stradia tables you can find the current duplications (daily active) and you can look deep for the data using the variables that we used."),
             br(),
             h5("We need to consider that some users can cheat us with different prices, titles, description, mileages or even year so is expected that we might have more duplicates than this report shows."),
             br(), 
             br(),
             h6("Date: 30 days for graphs, current day for tables"),
             h6("Source: Database"),
             h6("Author: Pedro Matos"))),  
  tabPanel('Storia Graph', plotOutput("duplicatesPlot")),   
  tabPanel('Storia Table', dataTableOutput('ex1')),
  tabPanel('Stradia Graph', plotOutput("duplicatesPlot2")),   
  tabPanel('Stradia Table', dataTableOutput('ex2'))
  
)

shinyApp(ui = ui, server = server)








>>>>>>> origin/master
