# #############################################################################
# # output to dashboardBody
# #############################################################################
# Key performance indicators using dynamic value boxes
row_migration <- fluidRow(
useShinyjs(),
     # first box for sales by quarter and region bar
     valueBoxOutput("dauBox", width = 3),
     valueBoxOutput("AvgSeBox", width = 3),
     valueBoxOutput("nnlBox", width = 3), 
     valueBoxOutput("nnlhourBox", width = 3))
  
 


tab_main_users_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="Daily Active Users",id="dauId",value='dauVal',
             box(
               title = "Daily Active Users"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("dauOnboarding")
             )),
    tabPanel(title="Avg Sessions",id="avgId",value='avgVal',
             box(
               title = "Avg Sessions"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("avgSessionOnboarding")
             )),
    tabPanel(title="NNL",id="nnlId",value='nnlVal',
             box(
               title = "NNL"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("nnlOnboarding")
             )),
    # tabPanel(title="Listers",id="listersId",value='listersVal',
    #          box(
    #            title = "Listers"
    #            ,status = "primary"
    #            ,solidHeader = FALSE 
    #            ,collapsible = FALSE
    #            ,width = 12
    #            ,htmlOutput("listersOnboarding")
    #          )), 

    tabPanel(title="NNL by Hour",id="nnlhourId",value='nnlhourVal',
             box(
               title = "NNL by Hour"
               ,status = "primary"
               ,solidHeader = FALSE
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("nnlhourOnboarding")
             ))

    
    
  )
  )
 
# #############################################################################
# # output to server function

tab_migration <- tabItem(tabName = "migration",row_migration,tab_main_users_line_evolution) 

# #############################################################################

server_migration <- function(input, output, session) {
 
 # fluid row 1, kpi 1: DAU
  output$dauBox <- renderValueBox({
        valueBox("1 481", "DAU",
           icon = icon("user"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: Avegare Session 
  output$AvgSeBox <- renderValueBox({
    valueBox("21:53", "Average Session",
             icon = icon("clock-o"),
             color = "blue")
  })
  

  # fluid row 1, kpi 5: NNL
  output$nnlBox <- renderValueBox({
    valueBox("3 125",
             "NNL",
             icon = icon("sign-in"),
             color = "maroon")
  })
  
  # fluid row 1, kpi 6: Listers 
  # output$listersBox <- renderValueBox({
  #   valueBox("3 293",
  #            "Active Listers",
  #            icon = icon("users"),
  #            color = "blue")
  # })
  
  # fluid row 1, kpi 7: NNL by Hour
  output$nnlhourBox <- renderValueBox({
    valueBox("","NNL by Hour",
             color = "olive")
  })


#    on click
#    # # on click of a tab1 valuebox
shinyjs::onclick('dauBox',expr={
     # move to tab2
updateTabsetPanel(session, "navbar", 'dauVal')
})

  #    # # on click of a tab2 valuebox
  shinyjs::onclick('AvgSeBox',expr={
    # move to tab2
   updateTabsetPanel(session, "navbar", 'avgVal')
  })
  


#  # # on click of a tab1 valuebox
shinyjs::onclick('nnlBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'nnlVal')
})

# #  # # on click of a tab1 valuebox
# shinyjs::onclick('listersBox',expr={
#   # move to tab2
#   updateTabsetPanel(session, "navbar", 'listersVal')
# })

# #  # # on click of a tab1 valuebox
shinyjs::onclick('nnlhourBox',expr={
#   # move to tab2
updateTabsetPanel(session, "navbar", 'nnlhourVal')
  })



#user plot 

output$dauOnboarding <- renderGvis({
  user <- gvisAreaChart(UsersAstotal, 
                        xvar = 'Date', yvar = 'DAU', options = list(
                          legend = 'none',
                          backgroundColor = "{fill:'transparent'}",
                          colors = "['#ff6600']"
                        ))
  user
  
})

#average Sessions plot 

output$avgSessionOnboarding <- renderGvis({
  avg <- gvisAreaChart(AvgSessionAtlas, 
                       xvar = 'Date', yvar = 'AVG Time on Site (Min)', options = list(
                         legend = 'none', 
#vAxes="[{viewWindowMode:'explicit',
#viewWindow:{min:0, max:370000}}]",
                         backgroundColor = "{fill:'transparent'}",
                         colors = "['#0066ff']"
                       ))              
  avg
})

### NNL 

output$nnlOnboarding <- renderGvis({
  nnl <- gvisAreaChart(NNLatlas, 
                           xvar = 'Date', yvar = 'NNL', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#990066']"
                           ))              
  nnl
})
# 
# output$listersOnboarding <- renderGvis({
#   listers <- gvisAreaChart(Activelisters, 
#                            xvar = 'Date', yvar = 'Active listers', options = list(
#                              legend = 'none',
#                              backgroundColor = "{fill:'transparent'}",
#                              colors = "['#336699']"
#                            ))              
#   listers
# })


output$nnlhourOnboarding <- renderGvis({
nnlhour <- gvisAreaChart(NNLhour,
                               xvar = 'Date', yvar = 'NNL', options = list(
                                 legend = 'none',
                                 backgroundColor = "{fill:'transparent'}",
                                 colors = "['#339966']"
                               ))
 nnlhour
})



}








 