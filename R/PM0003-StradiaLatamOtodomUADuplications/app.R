

library(ggplot2)
library(shiny)
library(scales)
library(googleVis)

server <- function(input, output) {
  
  
  #load date 
  load("Stradiardupfinal.RData")
  load("Stradiacodupfinal.RData")
  load("Stradiapedupfinal.RData")
  load("Stradiaecdupfinal.RData")
  load("Otodomuadupfinal.RData")
  load("dfstradiar.RData")
  load("dfstradiaco.RData")
  load("dfstradiape.RData")
  load("dfstradiaec.RData")
  load("dfotodomua.RData")
  
  
  
  #Ajust similarity percentage (Shiny has some problems with that)
  
  Otodomuadupfinal2$similarity <- paste(round(Otodomuadupfinal2$similarity*100,digits=1),"%",sep="")
  
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
  
  output$ex5 <- renderDataTable(
    Otodomuadupfinal2, options = list(pageLength = 30)
  )
  
  
  
  #plot (evolution by month)
  
  #Stradia Graphs 
  
  output$duplicatesPlot1 <- renderGvis({
    
    
    graphstradiar <- gvisComboChart(dfstradiar, xvar="Date", 
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
    
    graphstradiar
    
  })
  
  output$duplicatesPlot2 <- renderGvis({
    
    
    graphstradiaco <- gvisComboChart(dfstradiaco, xvar="Date", 
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
    
    graphstradiaco
    
  })
  
  output$duplicatesPlot3 <- renderGvis({
    
    
    graphstradiape <- gvisComboChart(dfstradiape, xvar="Date", 
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
    
    graphstradiape
    
  })
  
  output$duplicatesPlot4 <- renderGvis({
    
    
    graphstradiaec <- gvisComboChart(dfstradiaec, xvar="Date", 
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
    
    graphstradiaec
    
  })
  
  
  output$duplicatesPlot5 <- renderGvis({
    
    
    graphotodomua <- gvisComboChart(dfotodomua, xvar="Date", 
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
    
    graphotodomua
    
  })
 
}


ui <- navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Stradia Latam and Otodom UA Duplicated"),
           mainPanel(
             h5("This report provides data about ad duplications for Stradia Latam (Argentina, Colombia, Peru and Ecuador) and Otodom UA based on some variables."),
             br(),
             h5("To find ad duplicates we use the following variables for Stradia Latam: 
                Same user id, same brand, same model, same year, same fuel and mileage."),
             br(),
             h5("To find ad duplicates we use the following variables for Otodom UA: 
                Same user id, same city id, same category id, same price and title similarity with at least 70%."),
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
  tabPanel('Stradia Ar Dup Graph', htmlOutput("duplicatesPlot1")),   
  tabPanel('Stradia Ar Table', dataTableOutput('ex1')),
  tabPanel('Stradia Co Dup Graph', htmlOutput("duplicatesPlot2")),   
  tabPanel('Stradia Co Table', dataTableOutput('ex2')),
  tabPanel('Stradia Pe Dup Graph', htmlOutput("duplicatesPlot3")),   
  tabPanel('Stradia Pe Table', dataTableOutput('ex3')),
  tabPanel('Stradia Ec Dup Graph', htmlOutput("duplicatesPlot4")),   
  tabPanel('Stradia Ec Table', dataTableOutput('ex4')),
  tabPanel('Otodom UA Dup Graph', htmlOutput("duplicatesPlot5")),   
  tabPanel('Otodom UA Table', dataTableOutput('ex5'))
  
             )

shinyApp(ui = ui, server = server)








