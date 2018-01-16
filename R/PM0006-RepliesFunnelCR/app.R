


library(DT)
library(scales)
library(googleVis)

load("funnel_total_weeks.RData")


server <- function(input, output) {
  
  
  #load date 
  load("funnel_total_weeks.RData")
  load("funnel_total_otpl.RData")
  load("funnel_total_stro.RData")
  load("funnel_total_imo.RData")
  load("funnel_week_posting_otpl.RData")
  load("funnel_week_posting_stro.RData")
  load("funnel_week_posting_imo.RData")
  load("funnel_repliesplot_otpl.RData")
  load("funnel_repliesplot_stro.RData")
  load("funnel_repliesplot_imo.RData")

  #Ajust percentage (Shiny has some problems with that)
  
funnel_total_weeks$"Overall Conversion" <- paste(round(funnel_total_weeks$"Overall Conversion"*100,digits=1),"%",sep="")
funnel_total_weeks$"Step conversion" <- paste(round(funnel_total_weeks$"Step conversion"*100,digits=1),"%",sep="")

# 
# funnel_repliesplot_imo$`Home Replies CR` <- paste(round(funnel_repliesplot_imo$"Home Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_imo$`List Results Replies CR` <- paste(round(funnel_repliesplot_imo$"List Results Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_imo$`Ad Page Replies CR` <- paste(round(funnel_repliesplot_imo$"Ad Page Replies CR"*100,digits=1),"%",sep="")
# 
# 
# funnel_repliesplot_stro$`Home Replies CR` <- paste(round(funnel_repliesplot_stro$"Home Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_stro$`List Results Replies CR` <- paste(round(funnel_repliesplot_stro$"List Results Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_stro$`Ad Page Replies CR` <- paste(round(funnel_repliesplot_stro$"Ad Page Replies CR"*100,digits=1),"%",sep="")
# 
# 
# funnel_repliesplot_otpl$`Home Replies CR` <- paste(round(funnel_repliesplot_otpl$"Home Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_otpl$`List Results Replies CR` <- paste(round(funnel_repliesplot_otpl$"List Results Replies CR"*100,digits=1),"%",sep="")
# funnel_repliesplot_otpl$`Ad Page Replies CR` <- paste(round(funnel_repliesplot_otpl$"Ad Page Replies CR"*100,digits=1),"%",sep="")
# 
# 
#   
  #Tables

output$ex1 <- DT::renderDataTable(DT::datatable({
  data <- funnel_total_weeks
  if (input$plat != "All") {
    data <- data[data$Platform == input$plat,]
  }
  if (input$fun != "All") {
    data <- data[data$Funnel == input$fun,]
  }
  if (input$wek != "All") {
    data <- data[data$Week == input$wek,]
  }
  if (input$step != "All") {
    data <- data[data$Step == input$step,]
  }
  data
}))

output$funnelotplPlot <- renderGvis({
  
  
graphfunnelotpl <- gvisSankey(funnel_total_otpl, from="origin", 
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
  
  graphfunnelotpl
  
  })


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


output$funnelpostingotplPlot <- renderGvis({
  
  
  graphfunnelpostingotpl <- gvisSankey(funnel_week_posting_otpl, from="origin", 
                                to="to",weight="Step conversion",
                                options=list(
                                  height=700,width=1200,
                                  sankey="{link: {color: { fill: 'lightblue' } },
                                  node: { width: 30, 
                                  color: { fill: '#a61d4c' },
                                  label: { fontName: 'San Serif',
                                  fontSize: 15,
                                  color: 'black',
                                  bold: false,
                                  italic: true } }}"
                                ))
  
  graphfunnelpostingotpl
  
  })



output$funnelpostingstroPlot <- renderGvis({
  
  
graphfunnelpostingstro <- gvisSankey(funnel_week_posting_stro, from="origin", 
                                       to="to",weight="Step conversion",
                                       options=list(
                                         height=700,width=1200,
                                         sankey="{link: {color: { fill: 'lightblue' } },
                                         node: { width: 30, 
                                         color: { fill: '#a61d4c' },
                                         label: { fontName: 'San Serif',
                                         fontSize: 15,
                                         color: 'black',
                                         bold: false,
                                         italic: true } }}"
                                       ))
  
  graphfunnelpostingstro
  
  })



output$funnelpostingimoPlot <- renderGvis({
  
  
graphfunnelpostingimo <- gvisSankey(funnel_week_posting_imo, from="origin", 
                                       to="to",weight="Step conversion",
                                       options=list(
                                         height=700,width=1200,
                                         sankey="{link: {color: { fill: 'lightblue' } },
                                         node: { width: 30, 
                                         color: { fill: '#a61d4c' },
                                         label: { fontName: 'San Serif',
                                         fontSize: 15,
                                         color: 'black',
                                         bold: false,
                                         italic: true } }}"
                                       ))
  
  graphfunnelpostingimo
  
  })

output$funnelrepliesimoPlot <- renderGvis({
  
graphfunnelrepliesimo <- gvisLineChart(funnel_repliesplot_imo, 
              xvar = 'Weeks', yvar = c('Home Replies CR','List Results Replies CR','Ad Page Replies CR'), options = list(
                height=500,width=900,
                backgroundColor = "{fill:'transparent'}",
                colors = "['#0066ff','#339999','#333399']", 
                titleTextStyle="{color:'black',
  fontName:'Courier',
                fontSize:16}",
                vAxes="[{title:'%Conversion Rate',
                format:'#,###%',
                titleTextStyle: {color: 'black'},
                textStyle:{color: 'black'},
                textPosition: 'out',
                minValue:0}]",
                hAxes="[{title:'Date',
                textPosition: 'out'}]"
              ))      

graphfunnelrepliesimo

})

output$funnelrepliesotplPlot <- renderGvis({
  
  graphfunnelrepliesotpl <- gvisLineChart(funnel_repliesplot_otpl, 
                                         xvar = 'Weeks', yvar = c('Home Replies CR','List Results Replies CR','Ad Page Replies CR'), options = list(
                                           height=500,width=900,
                                           backgroundColor = "{fill:'transparent'}",
                                           colors = "['#0066ff','#339999','#333399']", 
                                           titleTextStyle="{color:'black',
                                           fontName:'Courier',
                                           fontSize:16}",
                                           vAxes="[{title:'%Conversion Rate',
                                           format:'#,###%',
                                           titleTextStyle: {color: 'black'},
                                           textStyle:{color: 'black'},
                                           textPosition: 'out',
                                           minValue:0}]",
                                           hAxes="[{title:'Date',
                                           textPosition: 'out'}]"
                                         ))      
  
  graphfunnelrepliesotpl
  
                                           })


output$funnelrepliestroPlot <- renderGvis({
  
  graphfunnelrepliestro <- gvisLineChart(funnel_repliesplot_stro, 
                                         xvar = 'Weeks', yvar = c('Home Replies CR','List Results Replies CR','Ad Page Replies CR'), options = list(
                                           height=500,width=900,
                                           backgroundColor = "{fill:'transparent'}",
                                           colors = "['#0066ff','#339999','#333399']", 
                                           titleTextStyle="{color:'black',
  fontName:'Courier',
                fontSize:16}",
                                           vAxes="[{title:'%Conversion Rate',
                format:'#,###%',
                titleTextStyle: {color: 'black'},
                textStyle:{color: 'black'},
                textPosition: 'out',
                minValue:0}]",
                                           hAxes="[{title:'Date',
                textPosition: 'out'}]"
                                         ))      
  
  graphfunnelrepliestro
  
})

}


ui <- navbarPage(
  "Funnels",
  tabPanel("Table",
    # Give the page a title
    titlePanel(""),
    
      
      # Create a new Row in the UI for selectInputs
      fixedRow(
        column(width=2,
               selectInput("plat",
                           "Platform:",
                           c("All",
                             unique(as.character(funnel_total_weeks$Platform))))
        ),
        column(width=2,
               selectInput("fun",
                           "Funnel:",
                           c("All",
                             unique(as.character(funnel_total_weeks$Funnel))))
        ),
        column(width=2,
               selectInput("wek",
                           "Week:",
                           c("All",
                             unique(as.character(funnel_total_weeks$Week))))
        )
      ),
    column(width=2,
           selectInput("step",
                       "Step:",
                       c("All",
                         unique(as.character(funnel_total_weeks$Step))))
  
  ),
    
      # Create a new row for the table.
      fluidRow(
        DT::dataTableOutput("ex1")
      )), 
tabPanel('Otodom PL Funnels Graphs',
         tabsetPanel(type = "tabs",
                     tabPanel("Replies Funnel - Week 1-18",htmlOutput("funnelotplPlot")),
                     tabPanel("Posting Funnel - Week 1-18",htmlOutput("funnelpostingotplPlot")), 
                     tabPanel("Overall CR Replies Evolution",htmlOutput("funnelrepliesotplPlot"))
                     )), 
tabPanel('Imovirtual Funnels Graphs',
         tabsetPanel(type = "tabs",
                     tabPanel("Replies Funnel - Week 1-18",htmlOutput("funnelimoPlot")),
                     tabPanel("Posting Funnel - Week 1-18",htmlOutput("funnelpostingimoPlot")),
                     tabPanel("Overall CR Replies Evolution",htmlOutput("funnelrepliesimoPlot"))
         )), 
tabPanel('Storia RO Funnels Graphs',
         tabsetPanel(type = "tabs",
                     tabPanel("Replies Funnel - Week 1-18",htmlOutput("funnelstroPlot")),
                     tabPanel("Posting Funnel - Week 1-18",htmlOutput("funnelpostingstroPlot")),
                     tabPanel("Overall CR Replies Evolution",htmlOutput("funnelrepliestroPlot"))
         ))
) 

shinyApp(ui = ui, server = server)








