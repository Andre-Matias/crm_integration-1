

library(ggplot2)
library(shiny)
library(scales)
library(googleVis)


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
  
  output$duplicatesPlot <- renderGvis({
    
graphstoria <- gvisComboChart(df, xvar="Date", 
                              yvar=c("%Duplicates", "Ads"),
                                  options=list(title="%Duplicates by Active Ads",
                                               titleTextStyle="{color:'black',
                                               fontName:'Courier',
                                               fontSize:16}",
                                               curveType="function", 
                                               pointSize=9,
                                               seriesType="bars",
                                               series="[{type:'line', 
                                               targetAxisIndex:0,
                                               color:'blue'}, 
                                               {type:'bars', 
                                               targetAxisIndex:1,
                                               color:'orange'}]",
                                               vAxes="[{title:'Percent',
                                               format:'#,###%',
                                               titleTextStyle: {color: 'black'},
                                               textStyle:{color: 'black'},
                                               textPosition: 'out'}, 
                                               {title:'Thousands',
                                               format:'#,###',
                                               titleTextStyle: {color: 'black'},
                                               textStyle:{color: 'black'},
                                               textPosition: 'out',
                                               minValue:0}]",
                                               hAxes="[{title:'Date',
                                               textPosition: 'out'}]",
                                               width=1200, height=600
                                  ))
    
  graphstoria
    
  })
  

  #Stradia Graph
  output$duplicatesPlot2 <- renderGvis({
    
    
    graphstradia <- gvisComboChart(dfstradia, xvar="Date", 
                                  yvar=c("%Duplicates", "Ads"),
                                  options=list(title="%Duplicates by Active Ads",
                                               titleTextStyle="{color:'black',
                                               fontName:'Courier',
                                               fontSize:16}",
                                               curveType="function", 
                                               pointSize=9,
                                               seriesType="bars",
                                               series="[{type:'line', 
                                               targetAxisIndex:0,
                                               color:'blue'}, 
                                               {type:'bars', 
                                               targetAxisIndex:1,
                                               color:'orange'}]",
                                               vAxes="[{title:'Percent',
                                               format:'#,###%',
                                               titleTextStyle: {color: 'black'},
                                               textStyle:{color: 'black'},
                                               textPosition: 'out',
                                               minValue:0}, 
                                               {title:'Thousands',
                                               format:'#,###',
                                               titleTextStyle: {color: 'black'},
                                               textStyle:{color: 'black'},
                                               textPosition: 'out',
                                               minValue:0}]",
                                               hAxes="[{title:'Date',
                                               textPosition: 'out'}]",
                                               width=1200, height=600
                                  ))
    
    graphstradia
    
  })
  
  output$DeletedPlot <- renderGvis({
    
    graphstoriadel <- gvisLineChart(dfstoriadel1, xvar="Date", yvar=c("Deleted 1 Day %","Deleted 3 Days %","Deleted 7 Days %"),
                                    options=list(title="%Deleted ads based on creation date of the last 7, 3 and 1 Day",
                                                 titleTextStyle="{color:'black',
                                                 fontName:'Courier',
                                                 fontSize:16}",
                                                 vAxes="[{title:'%Deleted Ads',
                                                 format:'#,###%',
                                                 titleTextStyle: {color: 'black'},
                                                 textStyle:{color: 'black'},
                                                 textPosition: 'out',
                                                 minValue:0}]",
                                                 hAxes="[{title:'Date',
                                                 textPosition: 'out'}]",
                                                 width=1200, height=600
                                    ))
    
    
graphstoriadel
    
  }) 
  
  output$DeletedPlot2 <- renderGvis({
  
    graphstradiadel <- gvisLineChart(dfstradiadel1, xvar="Date", yvar=c("Deleted 1 Day %","Deleted 3 Days %","Deleted 7 Days %"),
                                    options=list(title="%Deleted ads based on creation date of the last 7, 3 and 1 Day",
                                                 titleTextStyle="{color:'black',
                                                 fontName:'Courier',
                                                 fontSize:16}",
                                                 vAxes="[{title:'%Deleted Ads',
                                                 format:'#,###%',
                                                 titleTextStyle: {color: 'black'},
                                                 textStyle:{color: 'black'},
                                                 textPosition: 'out',
                                                 minValue:0}]",
                                                 hAxes="[{title:'Date',
  textPosition: 'out'}]",
                                                 width=1200, height=600
                                    ))
    
    
graphstradiadel
  
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
  tabPanel('Storia Dup Graph', htmlOutput("duplicatesPlot")),   
  tabPanel('Storia Dup Table', dataTableOutput('ex1')),
  tabPanel('Stradia Dup Graph', htmlOutput("duplicatesPlot2")),   
  tabPanel('Stradia Dup Table', dataTableOutput('ex2')),
  tabPanel('Storia Del Graph', htmlOutput("DeletedPlot")),
  tabPanel('Stradia Del Graph',htmlOutput("DeletedPlot2"))
  
)

shinyApp(ui = ui, server = server)








