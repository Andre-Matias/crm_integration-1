

library(ggplot2)
library(shiny)
library(scales)

server <- function(input, output) {
  
  
  #load date 
  load("Stradiardupfinal.RData")
  load("Stradiacodupfinal.RData")
  load("Stradiapedupfinal.RData")
  load("Stradiaecdupfinal.RData")
  load("dfstradiar.RData")
  load("dfstradiaco.RData")
  load("dfstradiape.RData")
  load("dfstradiaec.RData")
  
  
  #Tables(daily duplicates)
  
  output$ex1 <- renderDataTable(
    Stradiardupfinal3, options = list(pageLength = 30)
  )
  
  output$ex2 <- renderDataTable(
    Stradiacodupfinal3, options = list(pageLength = 30)
  )
  
  output$ex3 <- renderDataTable(
    Stradiapedupfinal3, options = list(pageLength = 30)
  )
  
  output$ex4 <- renderDataTable(
    Stradiaecdupfinal3, options = list(pageLength = 30)
  )
  
  
  
  #plot (evolution by month)
  
  #Stradia Graphs 
  
  options(scipen=10000)
  
  output$duplicatesPlot1 <- renderPlot({
    
    ggplot(dfstradiar, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 5700)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  })
  
  output$duplicatesPlot2 <- renderPlot({
    
    ggplot(dfstradiaco, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 4500)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  })
  
  output$duplicatesPlot3 <- renderPlot({
    
    ggplot(dfstradiape, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 150)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  })
  
  output$duplicatesPlot4 <- renderPlot({
    
    ggplot(dfstradiaec, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 1600)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  })
  
  
 
}


ui <- navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Stradia Latam Duplicated"),
           mainPanel(
             h5("This report provides data about ad duplications and deleted for Stradia Latam (Argentina, Colombia, Peru and Ecuador) based on some variables."),
             br(),
             h5("To find ad duplicates we use the following variables: 
                Same user id, same brand, same model, same year and same fuel."),
             br(),
             h5("We have the plots with the relation between active ads and ad duplicates, by day. 
                Due performance and memory capacity reasons, we just consider a maximum of 30 days per plot. With this we can have an overall perspective regarding the evolution of duplicates."), 
             br(),
             h5("On tables you can find the current duplications (daily active) and you can look deep for the data using the variables that we used."),
             br(), 
             br(),
             h6("Date: 30 days for graphs, current day for tables"),
             h6("Source: Database"),
             h6("Author: Pedro Matos"))),  
  tabPanel('Stradia Ar Dup Graph', plotOutput("duplicatesPlot1")),   
  tabPanel('Stradia Ar Table', dataTableOutput('ex1')),
  tabPanel('Stradia Co Dup Graph', plotOutput("duplicatesPlot2")),   
  tabPanel('Stradia Co Table', dataTableOutput('ex2')),
  tabPanel('Stradia Pe Dup Graph', plotOutput("duplicatesPlot3")),   
  tabPanel('Stradia Pe Table', dataTableOutput('ex3')),
  tabPanel('Stradia Ec Dup Graph', plotOutput("duplicatesPlot4")),   
  tabPanel('Stradia Ec Table', dataTableOutput('ex4'))
  
             )

shinyApp(ui = ui, server = server)








