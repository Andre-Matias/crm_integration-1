

library(ggplot2)
library(shiny)
library(scales)
library(googleVis)


server <- function(input, output) {
  
  
  #load date 
  load("funnel_total_imo.RData")
  load("funnel_total_imo_per.RData")
  load("funnel_total_otpl.RData")
  load("funnel_total_otpl_per.RData")
  load("funnel_total_stro.RData")
  load("funnel_total_stro_per.RData")
 
  
  #Ajust similarity percentage (Shiny has some problems with that)
  
funnel_total_imo_per$"Overall Conversion" <- paste(round(funnel_total_imo_per$"Overall Conversion"*100,digits=1),"%",sep="")
funnel_total_imo_per$"Step conversion" <- paste(round(funnel_total_imo_per$"Step conversion"*100,digits=1),"%",sep="")

funnel_total_otpl_per$"Overall Conversion" <- paste(round(funnel_total_otpl_per$"Overall Conversion"*100,digits=1),"%",sep="")
funnel_total_otpl_per$"Step conversion" <- paste(round(funnel_total_otpl_per$"Step conversion"*100,digits=1),"%",sep="")

funnel_total_stro_per$"Overall Conversion" <- paste(round(funnel_total_stro_per$"Overall Conversion"*100,digits=1),"%",sep="")
funnel_total_stro_per$"Step conversion" <- paste(round(funnel_total_stro_per$"Step conversion"*100,digits=1),"%",sep="")


  
  #Tables(daily duplicates)
  
  output$ex1 <- renderDataTable(
    funnel_total_otpl_per, options = list(pageLength = 10)
  )
  
  output$ex2 <- renderDataTable(
   funnel_total_imo_per, options = list(pageLength = 10)
  )
  
  output$ex3 <- renderDataTable(
    funnel_total_stro_per, options = list(pageLength = 10)
  )
  

  #plot (evolution by month)
  
  #Otodom PL Graph 
  
  output$funnelotplPlot <- renderGvis({
    
   
    graphfunnelotpl <- gvisSankey(funnel_total_stro, from="origin", 
                 to="to",weight="Step conversion",
                 options=list(
                   height=600,width=950,
                   sankey="{link: {color: { fill: 'lightblue' } },
                        node: { width: 30, 
               color: { fill: '#a61d4c' },
               label: { fontName: 'San Serif',
               fontSize: 15,
               color: 'black',
               bold: false,
               italic: true } }}"
                   ))
      
  graphfunnelotpl
    
  })
  

  #Imovirtual Graph 

  
  output$funnelimoPlot <- renderGvis({
    
    
    graphfunnelimo <- gvisSankey(funnel_total_imo, from="origin", 
                                  to="to",weight="Step conversion",
                                  options=list(
                                    height=800,width=950,
                                    sankey="{link: {color: { fill: 'lightblue' } },
                                    node: { width: 30, 
                                    color: { fill: '#a61d4c' },
                                    label: { fontName: 'San Serif',
                                    fontSize: 15,
                                    color: 'black',
                                    bold: false,
                                    italic: true } }}"
                                  ))
    
    graphfunnelimo
    
    })
  
  
  
  #Storia RO Graph 
  
  
  output$funnelstroPlot <- renderGvis({
    
    
    graphfunnelstro <- gvisSankey(funnel_total_stro, from="origin", 
                                 to="to",weight="Step conversion",
                                 options=list(
                                   height=800,width=950,
                                   sankey="{link: {color: { fill: 'lightblue' } },
                                   node: { width: 30, 
                                   color: { fill: '#a61d4c' },
                                   label: { fontName: 'San Serif',
                                   fontSize: 15,
                                   color: 'black',
                                   bold: false,
                                   italic: true } }}"
                                 ))
    
    graphfunnelstro

  })
  
  
}
  
ui <- navbarPage(
  title = ("Replies Funnel Conversion Rate"),
  tabPanel('Otodom PL Funnel Graph', htmlOutput("funnelotplPlot")),   
  tabPanel('Otodom PL Funnel Table', dataTableOutput('ex1')),
  tabPanel('Imovirtual Funnel Graph', htmlOutput("funnelimoPlot")),   
  tabPanel('Imovirtual Funnel Table', dataTableOutput('ex2')),
  tabPanel('Storia RO Funnel Graph', htmlOutput("funnelstroPlot")),
  tabPanel('Storia RO Funnel Table',dataTableOutput('ex3'))
  
)

shinyApp(ui = ui, server = server)








