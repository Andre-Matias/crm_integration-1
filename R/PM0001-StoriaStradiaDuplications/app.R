

library(ggplot2)
library(shiny)
library(scales)

server <- function(input, output) {
  
  
  #load date 
  load("dfstoriadup.RData")
  load("Storiadupfinal.RData")
  load("dfstradia.RData")
  load("Stradiadupfinal.RData")
  load("dfstoriadel1.RData")
  load("dfstradiadel1.RData")
  
  
  #Ajust similarity percentage (Shiny has some problems with that)
  
  Storiadupfinal2$similarity <- paste(round(Storiadupfinal2$similarity*100,digits=1),"%",sep="")
  
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
      coord_cartesian(ylim = c(30000, 560000)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=0,angle=90)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") +
      theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  })
  

  #Stradia Graph
  output$duplicatesPlot2 <- renderPlot({
    
    ggplot(dfstradia, aes(Date)) + 
      geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
      geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
      scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
      coord_cartesian(ylim = c(0, 53000)) + 
      geom_text(aes(y= Duplicates,label =paste0(Duplicates,"\n",perduplicates), vjust=-1)) +
      geom_text(aes(y= Ads,label = Ads, vjust=2)) +
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("Active Ads vs Active Duplicates by Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  })
  
  output$DeletedPlot <- renderPlot({
    
    ggplot(dfstoriadel1, aes(Date)) +
      geom_line(aes(y = dfstoriadel1$"Deleted 7 Days %", group = 1, color = "Deleted 7 Days %")) +  
      geom_line(aes(y = dfstoriadel1$"Deleted 3 Days %",group = 1, color = "Deleted 3 Days %")) +
      geom_line(aes(y = dfstoriadel1$"Deleted 1 Day %", group = 1, color = "Deleted 1 Day %")) +
      scale_colour_manual("", values=c("Deleted 3 Days %" = "blue","Deleted 7 Days %" = "orange","Deleted 1 Day %" = "brown")) +
      geom_text(aes(y= dfstoriadel1$"Deleted 7 Days %",label = percent(dfstoriadel1$"Deleted 7 Days %"), vjust=1)) + 
      geom_text(aes(y= dfstoriadel1$"Deleted 3 Days %",label = percent(dfstoriadel1$"Deleted 3 Days %"), vjust=1)) + 
      geom_text(aes(y= dfstoriadel1$"Deleted 1 Day %",label = percent(dfstoriadel1$"Deleted 1 Day %"), vjust=1)) + 
      ylab("Deleted Ads %") + scale_y_continuous(breaks = seq(0, 1, 0.01),labels=percent) + 
      scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
      ggtitle("%Deleted ads based on creation date of the last 7, 3 and 1 Day") + 
      theme(plot.title = element_text(lineheight=.8, face="bold")) 
    
  }) 
  
  output$DeletedPlot2 <- renderPlot({
  
  ggplot(dfstradiadel1, aes(Date)) +
    geom_line(aes(y = dfstradiadel1$"Deleted 7 Days %", group = 1, color = "Deleted 7 Days %")) +  
    geom_line(aes(y = dfstradiadel1$"Deleted 3 Days %",group = 1, color = "Deleted 3 Days %")) +
    geom_line(aes(y = dfstradiadel1$"Deleted 1 Day %", group = 1, color = "Deleted 1 Day %")) +
    scale_colour_manual("", values=c("Deleted 3 Days %" = "blue","Deleted 7 Days %" = "orange","Deleted 1 Day %" = "brown")) +
    geom_text(aes(y= dfstradiadel1$"Deleted 7 Days %",label = percent(dfstradiadel1$"Deleted 7 Days %"), vjust=1)) + 
    geom_text(aes(y= dfstradiadel1$"Deleted 3 Days %",label = percent(dfstradiadel1$"Deleted 3 Days %"), vjust=1)) + 
    geom_text(aes(y= dfstradiadel1$"Deleted 1 Day %",label = percent(dfstradiadel1$"Deleted 1 Day %"), vjust=1)) + 
    ylab("Deleted Ads %") + scale_y_continuous(breaks = seq(0, 1, 0.2),labels=percent) + 
    scale_x_date(date_breaks="2 days", date_labels="%d%b") + 
    ggtitle("%Deleted ads based on creation date of the last 7, 3 and 1 Day") + 
    theme(plot.title = element_text(lineheight=.8, face="bold"))  
  
})
  
}

  
ui <- navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Storia.IND and Stradia.IN Duplicated and Deleted Ads"),
           mainPanel(
             h5("This report provides data about ad duplications and deleted for Storia Indonesia and Stradia India based on some variables."),
             br(),
             h5("To find ad duplicates we use the following variables for Storia: 
                Same user id, same city id, same category id, same price and title similarity with at least 70%.
                For Stradia we use the following variables:
                Same user id, same brand, same model, same year, same fuel and same Variant."),
             br(),
             h5("In Storia and Stradia dup graph tabs we have a plot with the relation between active ads and ad duplicates, by day. 
                Due performance and memory capacity reasons, we just consider a maximum of 30 days per plot. With this we can have an overall perspective regarding the evolution of duplicates."), 
             br(),
             h5("On Storia and Stradia del graphs we can see the % of deleted ads considering the creation date (NNL) for the last 7, 3 and 1 day."),
             br(),
             h5("On Storia and Stradia tables you can find the current duplications (daily active) and you can look deep for the data using the variables that we used.
                These differences between both platforms help us to understand why Stradia has such a low number of active duplicates."),
             br(),
             h5("We need to consider that some users can cheat us with different prices, titles, description, mileages or even year so is expected that we might have more duplicates than this report shows."),
             br(), 
             br(),
             h6("Date: 30 days for graphs, current day for tables"),
             h6("Source: Database"),
             h6("Author: Pedro Matos"))),  
  tabPanel('Storia Dup Graph', plotOutput("duplicatesPlot")),   
  tabPanel('Storia Dup Table', dataTableOutput('ex1')),
  tabPanel('Stradia Dup Graph', plotOutput("duplicatesPlot2")),   
  tabPanel('Stradia Dup Table', dataTableOutput('ex2')),
  tabPanel('Storia Del Graph',plotOutput("DeletedPlot")),
  tabPanel('Stradia Del Graph',plotOutput("DeletedPlot2"))
  
)

shinyApp(ui = ui, server = server)








