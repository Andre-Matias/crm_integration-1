# #############################################################################
# # output to dashboardBody
# #############################################################################
# Key performance indicators using dynamic value boxes
row_migration <- fluidRow(
useShinyjs(),
     # first box for sales by quarter and region bar
    valueBoxOutput("totalvasBox", width = 3), 
    valueBoxOutput("vasBox", width = 3),
    valueBoxOutput("b2cfunnelBox", width = 3),
    valueBoxOutput("totalrevBox", width = 3), 
    valueBoxOutput("revenueBox", width = 3))
  
 


tab_main_users_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
  tabPanel(title="Total VAS",id="totalvasId",value='totalvasVal',
           box(
             title = "Total B2C VAS"
             ,status = "primary"
             ,solidHeader = FALSE 
             ,collapsible = FALSE
             ,width = 12
             ,htmlOutput("totalvasOnboarding")
           )), 
  tabPanel(title="VAS",id="vasId",value='vasVal',
           box(
             title = "B2C VAS"
             ,status = "primary"
             ,solidHeader = FALSE 
             ,collapsible = FALSE
             ,width = 12
             ,htmlOutput("vasOnboarding")
           )),
    tabPanel(title="B2C Payment Funnel",id="b2cfunnelId",value='b2cfunnelVal',
             box(
               title = "B2C Payment Funnel"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("b2cfunnelOnboarding")
             )),
    tabPanel(title="Total B2C VAS Revenue",id="totalrevId",value='totalrevVal',
             box(
               title = "Total B2C VAS Revenue"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("totalrevOnboarding")
             )), 
  tabPanel(title="B2C VAS Revenue",id="revenueId",value='revenueVal',
           box(
             title = "B2C VAS Revenue"
             ,status = "primary"
             ,solidHeader = FALSE 
             ,collapsible = FALSE
             ,width = 12
             ,htmlOutput("revenueOnboarding")
           ))
  
    
)
)
  
 
# #############################################################################
# # output to server function

tab_migration <- tabItem(tabName = "migration",row_migration,tab_main_users_line_evolution) 

# #############################################################################

server_migration <- function(input, output, session) {
 
 # fluid row 1, kpi 1: Total VAS Volume
  output$totalvasBox <- renderValueBox({
        valueBox("-1%", "Total B2C VAS Purchases (vs Last Week)",
           icon = icon("shopping-cart"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: VAS Volume 
  output$vasBox <- renderValueBox({
    valueBox("-",
             "Splitted B2C VAS Purchases",
             icon = icon("check-circle"),
             color = "aqua")
  })
  
  
  # fluid row 1, kpi 3: B2C Funnel 
  output$b2cfunnelBox <- renderValueBox({
    valueBox("37%",
             "Overall B2C Payment Funnel",
             icon = icon("filter"),
             color = "green")
  })
  
  # fluid row 1, kpi 4: Total Revenue 
  output$totalrevBox <- renderValueBox({
    valueBox("+4%",
             "Total B2C VAS Revenue (vs Last Week)",
             icon = icon("money"),
             color = "purple")
  })
  
  # fluid row 1, kpi 5: Revenue 
  output$revenueBox <- renderValueBox({
    valueBox("-",
             "Splitted B2C VAS Revenue",
             icon = icon("money"),
             color = "blue")
  })

#    on click
#    # # on click of a tab1 valuebox
shinyjs::onclick('totalvasBox',expr={
     # move to tab2
updateTabsetPanel(session, "navbar", 'totalvasVal')
})

  
  #    # # on click of a tab2 valuebox
  shinyjs::onclick('vasBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'vasVal')
  })
  
  #    # # on click of a tab2 valuebox
  shinyjs::onclick('b2cfunnelBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'b2cfunnelVal')
  })

#    # # on click of a tab2 valuebox
shinyjs::onclick('totalrevBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'totalrevVal')
})

#    # # on click of a tab2 valuebox
shinyjs::onclick('revenueBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'revenueVal')
})


#Total B2c VAS Volume 


output$totalvasOnboarding <- renderGvis({
  total <- gvisAreaChart(rovastotalv,
                         xvar = 'Week', yvar = c('Total VAS'), options = list(
                           legend = 'yes',
                           title="Total VAS by Week",
                           #vAxes="[{viewWindowMode:'explicit',
                           #viewWindow:{min:0, max:370000}}]",
                           width=1200, height=600,
                           vAxes="[{title:'Purchases',
                           format:'##'}]",
                           hAxes="[{title:'Week',
                           textPosition: 'out'}]",
                           colors = "['#FF9933']",
                           backgroundColor = "{fill:'transparent'}")
                         
)
  
  total
  
  })



#VAS B2C Volume plot 

output$vasOnboarding <- renderGvis({
vas <- gvisLineChart(rovasv,
                         xvar = 'Week', yvar = c('ad_homepage','highlight','header','pushup','mirror'), options = list(
                            legend = 'yes',
                            title="VAS by Week",
                            #vAxes="[{viewWindowMode:'explicit',
                            #viewWindow:{min:0, max:370000}}]",
                            width=1200, height=600,
                            vAxes="[{title:'Purchases',
                            format:'##'}]",
                            hAxes="[{title:'Week',
                            textPosition: 'out'}]",
                            backgroundColor = "{fill:'transparent'}"))

  
vas
  
})


#B2C Payment Funnel 

output$b2cfunnelOnboarding <- renderGvis({

b2cfunnel <- gvisSankey(funnel_b2c_payment_str, from="origin", 
           to="to",weight="Step conversion",
           options=list(
             height=500,width=1200,
             sankey="{link: {color: { fill: 'lightblue' } },
             node: { width: 30, 
             color: { fill: '#a61d4c' },
             label: { fontName: 'San Serif',
             fontSize: 15,
             color: 'black',
             bold: false,
             italic: true } }}"))

b2cfunnel
})



#Total B2C Revenue 


output$totalrevOnboarding <- renderGvis({
  totalrevenue <-gvisAreaChart(rorevtotal,
                                 xvar = 'Week', yvar = c('Total Revenue'), options = list(
                                   legend = 'yes',
                                   title="Total Revenue by Week",
                                   #vAxes="[{viewWindowMode:'explicit',
                                   #viewWindow:{min:0, max:370000}}]",
                                   width=1200, height=600,
                                   vAxes="[{title:'Revenue',
                                   format:'##'}]",
                                   hAxes="[{title:'Week',
                                   textPosition: 'out'}]",
                                   backgroundColor = "{fill:'transparent'}")
)
  
  totalrevenue
})


#B2C Revenue


output$revenueOnboarding <- renderGvis({
  
  revenue <- gvisLineChart(rorevas,
                        xvar = 'Week', yvar = c('ad_homepage','highlight','header','pushup','mirror'), options = list(
                          legend = 'yes',
                          title="Revenue by Week",
                          #vAxes="[{viewWindowMode:'explicit',
                          #viewWindow:{min:0, max:370000}}]",
                          width=1200, height=600,
                          vAxes="[{title:'Revenue',
                          format:'##'}]",
                          hAxes="[{title:'Week',
                          textPosition: 'out'}]",
                          backgroundColor = "{fill:'transparent'}")
                        )
  
  revenue
})


}



 