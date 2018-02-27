# #############################################################################
# # output to dashboardBody
# #############################################################################
# Key performance indicators using dynamic value boxes
row_migration <- fluidRow(
useShinyjs(),
     # first box for sales by quarter and region bar
     valueBoxOutput("transvBox", width = 3),
     valueBoxOutput("transrBox", width = 3),
     valueBoxOutput("transperBox", width = 3),
    valueBoxOutput("transfunnelBox", width = 3),
    valueBoxOutput("b2cfunnelBox", width = 3),
     valueBoxOutput("vasBox", width = 3),
valueBoxOutput("totalBox", width = 3), 
valueBoxOutput("revenueBox", width = 3)) 

  
 


tab_main_users_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="TM Purchases",id="transvId",value='transvVal',
             box(
               title = "TM Purchases"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("transvOnboarding")
             )),
    tabPanel(title="TM Revenue",id="transrId",value='transrVal',
             box(
               title = "TM Revenue"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("transrOnboarding")
             )),
    tabPanel(title="%TM by B2C VAS",id="transperId",value='transperVal',
             box(
               title = "%TM by B2C VAS"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("transperOnboarding")
             )),
    tabPanel(title="TM Payment Funnel",id="transfunnelId",value='transfunnelVal',
             box(
               title = "TM Payment Funnel"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("transfunnelOnboarding")
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
    tabPanel(title="Top VAS",id="vasId",value='vasVal',
             box(
               title = "Top B2C VAS"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("vasOnboarding")
             )), 
    tabPanel(title="Total VAS",id="totalvasId",value='totalvasVal',
             box(
               title = "Total B2C VAS"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("totalvasOnboarding")
             )),
    tabPanel(title="Total B2C VAS Revenue",id="totalrevenueId",value='totalrevenueVal',
             box(
               title = "Total B2C VAS Revenue"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("totalrevenueOnboarding")
             ))
    

  )
  )
 
# #############################################################################
# # output to server function

tab_migration <- tabItem(tabName = "migration",row_migration,tab_main_users_line_evolution) 

# #############################################################################

server_migration <- function(input, output, session) {
 
 # fluid row 1, kpi 1: TM Volume
  output$transvBox <- renderValueBox({
        valueBox("15", "Last Week TM Purchases",
           icon = icon("shopping-cart"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: TM Revenue 
  output$transrBox <- renderValueBox({
    valueBox("1 173 zl",
             "Last Week TM Revenue",
             icon = icon("money"),
             color = "aqua")
  })
  

  # fluid row 1, kpi 3: TM % Total 
  output$transperBox <- renderValueBox({
    valueBox("1%",
             "%TM by B2C VAS",
             icon = icon("percent"),
             color = "blue")
  })
  
  # fluid row 1, kpi 4: TM Funnel 
  output$transfunnelBox <- renderValueBox({
    valueBox("1%",
             "Overall TM Payment Funnel",
             icon = icon("filter"),
             color = "maroon")
  })
  
  
  # fluid row 1, kpi 5: B2C Funnel 
  output$b2cfunnelBox <- renderValueBox({
    valueBox("46%",
             "Overall B2C Payment Funnel",
             icon = icon("filter"),
             color = "yellow")
  })
  
  # fluid row 1, kpi 6: Top VAS
  output$vasBox <- renderValueBox({
    valueBox("-",
             "Top 3 B2C VAS",
             icon = icon("star"),
             color = "olive")
  })
  
  # fluid row 1, kpi 7: Total VAS 
  output$totalBox <- renderValueBox({
    valueBox("-",
             "Total B2C VAS",
             icon = icon("check-circle"),
             color = "purple")
  })
  
  # fluid row 1, kpi 2: TM Revenue 
  output$revenueBox <- renderValueBox({
    valueBox("-",
             "Total B2C VAS Revenue",
             icon = icon("money"),
             color = "teal")
  })
  

#    on click
#    # # on click of a tab1 valuebox
shinyjs::onclick('transvBox',expr={
     # move to tab2
updateTabsetPanel(session, "navbar", 'transvVal')
})

  #    # # on click of a tab2 valuebox
  shinyjs::onclick('transrBox',expr={
    # move to tab2
   updateTabsetPanel(session, "navbar", 'transrVal')
  })
  
  #    # # on click of a tab2 valuebox
  shinyjs::onclick('transperBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'transperVal')
  })
  
  #    # # on click of a tab2 valuebox
  shinyjs::onclick('transfunnelBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'transfunnelVal')
  })
  
  #    # # on click of a tab2 valuebox
  shinyjs::onclick('b2cfunnelBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'b2cfunnelVal')
  })
  
#    # # on click of a tab1 valuebox
shinyjs::onclick('vasBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'vasVal')
})

#    # # on click of a tab2 valuebox
shinyjs::onclick('totalBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'totalvasVal')
})

#    # # on click of a tab2 valuebox
shinyjs::onclick('revenueBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'totalrevenueVal')
})



#Transactional Volume plot 

output$transvOnboarding <- renderGvis({
  transv <- gvisAreaChart(otodomtransv,xvar = 'Week', yvar = c('trans_map_1','trans_map_30','trans_map_30_pre'), options = list(
    legend = 'yes', 
    title="Transactional Maps Purchases by Week",
    #vAxes="[{viewWindowMode:'explicit',
    #viewWindow:{min:0, max:370000}}]",
    width=1200, height=500, 
    vAxes="[{title:'Purchases',
    format:'##'}]",
    colors = "['#00CCCC','#FFB266','#3399FF']", 
    hAxes="[{title:'Week',
    textPosition: 'out'}]", 
    backgroundColor = "{fill:'transparent'}"))
  
 transv
  
})

#TM revenue plot

output$transrOnboarding <- renderGvis({
  transr <- gvisAreaChart(otodomtransr, 
                       xvar = 'Week', yvar = c('trans_map_1','trans_map_30','trans_map_30_pre'), options = list(
                         legend = 'yes', 
                         title="Transactional Maps Revenue by Week",
                         width=1200, height=500, 
                         vAxes="[{title:'Revenue (zl)',
                         format:'##'}]",
                         colors = "['#00CCCC','#FFB266','#3399FF']", 
                         hAxes="[{title:'Week',
                         textPosition: 'out'}]", 
                         backgroundColor = "{fill:'transparent'}")) 
  transr
})


# TM % Total 

output$transperOnboarding <- renderGvis({
  tmper <- gvisLineChart(otodomtranst,
                   xvar = 'Week', yvar = c('%Purchases','%Revenue'),
                   options = list(
                     legend = 'yes', 
                     title="%TM by Total B2C VAS by Week",
                     colors = "['#0066ff','#FF8000','#009999']", 
                     width=1200, height=500, 
                     vAxes="[{title:'%total B2C VAS',
                       format:'##'}]",
                     hAxes="[{title:'Week',
                       textPosition: 'out'}]", 
                     backgroundColor = "{fill:'transparent'}"))
tmper 
})
  
output$transfunnelOnboarding <- renderGvis({
  
  tmfunnel <- gvisSankey(funnel_trans_otpl, from="origin", 
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
  
  tmfunnel
})




output$b2cfunnelOnboarding <- renderGvis({

b2cfunnel <- gvisSankey(funnel_b2c_payment_otpl, from="origin", 
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



  
#Top 3 Otodom Vas plot 

output$vasOnboarding <- renderGvis({
  vas <- gvisAreaChart(otodomvasv,
                       xvar = 'Week', yvar = c('export_olx','pushup','topads'),
                       options = list(
                         legend = 'yes', 
                         title="Top 3 VAS Purchases by Week",
                         colors = "['#0066ff','#FF8000','#009999']", 
                         width=1200, height=500, 
                         vAxes="[{title:'Purchases',
                         format:'##'}]",
                         hAxes="[{title:'Week',
                         textPosition: 'out'}]", 
                         backgroundColor = "{fill:'transparent'}"))
                          
  vas
})



#Total VAS plot 

output$totalvasOnboarding <- renderGvis({
  total <- gvisComboChart(otodomvastotal,
                         xvar="Vas", 
                         yvar=c("Purchases", "Users"),
                         options=list(title="Total VAS Last 6 Weeks",
                                      titleTextStyle="{color:'black',
                                      fontName:'Courier',
                                      fontSize:16}",
                                      curveType="function", 
                                      pointSize=9,
                                      seriesType="bars",
                                      series="[{type:'line', 
                                      targetAxisIndex:0,
                                      color:'#009999'}, 
                                      {type:'bars', 
                                      targetAxisIndex:1,
                                      color:'#FFB266'}]",
                                      vAxes="[{title:'Purchases',
                                      format:'#,###',
                                      titleTextStyle: {color: 'black'},
                                      textStyle:{color: 'black'},
                                      textPosition: 'out',
                                      minValue:0}, 
                                      {title:'Users',
                                      format:'#,###',
                                      titleTextStyle: {color: 'black'},
                                      textStyle:{color: 'black'},
                                      textPosition: 'out',
                                      minValue:0}]",
                                      hAxes="[{title:'Vas',
                                      textPosition: 'out'}]",
                                      width=1200, height=500
                         ))     
  total
  })


output$totalrevenueOnboarding <- renderGvis({
 revenue <-gvisColumnChart(otodomrevenuetotal, 
                xvar = 'Vas', yvar = 'Revenue', options = list(
                  legend = 'yes', 
                  title="B2C VAS Revenue Last 6 Weeks",
                  #vAxes="[{viewWindowMode:'explicit',
                  #viewWindow:{min:0, max:370000}}]",
                  width=1200, height=500, 
                  vAxes="[{title:'Revenue (zl)',
                  format:'##'}]",
                  hAxes="[{title:'Vas',
                  textPosition: 'out'}]", 
                  colors = "['#00CCCC']", 
                  backgroundColor = "{fill:'transparent'}"))

 revenue
})

}


 