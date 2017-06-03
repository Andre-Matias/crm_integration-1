# #############################################################################
# # output to dashboardBody
# #############################################################################
# Key performance indicators using dynamic value boxes
row_migration <- fluidRow(
useShinyjs(),
     # first box for sales by quarter and region bar
     valueBoxOutput("dauBox", width = 3),
     valueBoxOutput("adsBox", width = 3),
     valueBoxOutput("revenueBox", width = 3),
     valueBoxOutput("repliesBox", width = 3), 
     valueBoxOutput("nnlBox", width = 3), 
     valueBoxOutput("listersBox", width = 3),
     valueBoxOutput("replieshourBox", width = 3),
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
    tabPanel(title="Active Ads",id="adsId",value='adsVal',
             box(
               title = "Active Ads"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("activeadsOnboarding")
             )),
    tabPanel(title="Revenue",id="revenueId",value='revenueVal',
             box(
               title = "Revenue"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("revenueOnboarding")
             )),
    tabPanel(title="Replies",id="repliesId",value='repliesVal',
             box(
               title = "Replies"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("repliesOnboarding")
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
    tabPanel(title="Listers",id="listersId",value='listersVal',
             box(
               title = "Listers"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("listersOnboarding")
             )), 
    tabPanel(title="Replies by Hour",id="replieshourId",value='replieshourVal',
             box(
               title = "Replies by Hour"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("replieshourOnboarding")
             )),
    
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
 
 # fluid row 1, kpi 2: DAU
  output$dauBox <- renderValueBox({
        valueBox("77 322", "DAU",
           icon = icon("user"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: Active ads
  output$adsBox <- renderValueBox({
    valueBox("303 237",
             "Active Ads",
             icon = icon("building-o"),
             color = "aqua")
  })
  
  # fluid row 1, kpi 3: Revenue
  output$revenueBox <- renderValueBox({
    valueBox("12 227",
             "Revenue",
             icon = icon("money"),
             color = "olive")
  })
  
  # fluid row 1, kpi 4: Replies
  output$repliesBox <- renderValueBox({
    valueBox("3 404",
             "Replies",
             icon = icon("reply"),
             color = "purple")
  })
  
  # fluid row 1, kpi 5: NNL
  output$nnlBox <- renderValueBox({
    valueBox("2 654",
             "NNL",
             icon = icon("sign-in"),
             color = "maroon")
  })
  
  # fluid row 1, kpi 6: Listers 
  output$listersBox <- renderValueBox({
    valueBox("3 514",
             "Active Listers",
             icon = icon("users"),
             color = "blue")
  })
  
  # fluid row 1, kpi 7: Replies by Hour
  output$replieshourBox <- renderValueBox({
    valueBox("","Replies by Hour",
             color = "purple")
  })
  
  # fluid row 1, kpi 7: NNL by Hour
  output$nnlhourBox <- renderValueBox({
    valueBox("","NNL by Hour",
             color = "maroon")
  })
  

#    on click
#    # # on click of a tab1 valuebox
shinyjs::onclick('dauBox',expr={
     # move to tab2
updateTabsetPanel(session, "navbar", 'dauVal')
})

  #    # # on click of a tab2 valuebox
  shinyjs::onclick('adsBox',expr={
    # move to tab2
   updateTabsetPanel(session, "navbar", 'adsVal')
  })
  
#    # # on click of a tab1 valuebox
shinyjs::onclick('revenueBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'revenueVal')
})

#  # # on click of a tab1 valuebox
shinyjs::onclick('repliesBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'repliesVal')
})

#  # # on click of a tab1 valuebox
shinyjs::onclick('nnlBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'nnlVal')
})

#  # # on click of a tab1 valuebox
shinyjs::onclick('listersBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'listersVal')
})

#  # # on click of a tab1 valuebox
shinyjs::onclick('replieshourBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'replieshourVal')
})

#  # # on click of a tab1 valuebox
shinyjs::onclick('nnlhourBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'nnlhourVal')
})



#user plot 

output$dauOnboarding <- renderGvis({
  user <- gvisAreaChart(UsersTotaltoday, 
                        xvar = 'Date', yvar = 'DAU', options = list(
                          legend = 'none',
                          backgroundColor = "{fill:'transparent'}",
                          colors = "['#ff6600']"
                        ))
  user
  
})

#active ads plot 

output$activeadsOnboarding <- renderGvis({
  ads <- gvisAreaChart(Activeads, 
                       xvar = 'Date', yvar = 'Active ads', options = list(
                         legend = 'none', 
#vAxes="[{viewWindowMode:'explicit',
#viewWindow:{min:0, max:370000}}]",
                         backgroundColor = "{fill:'transparent'}",
                         colors = "['#66ccff']"
                       ))              
  ads
})

#revenue plot 

output$revenueOnboarding <- renderGvis({
  revenue <- gvisAreaChart(Imorevenue, 
                           xvar = 'Date', yvar = 'Revenue', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#336633']"
                           ))              
  revenue
})

#Replies plot

output$repliesOnboarding <- renderGvis({
  replies <- gvisAreaChart(Imoreplies, 
                           xvar = 'Date', yvar = 'Replies', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#663366']"
                           ))              
  replies
})

output$nnlOnboarding <- renderGvis({
  nnl <- gvisAreaChart(NNL, 
                           xvar = 'Date', yvar = 'NNL', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#990066']"
                           ))              
  nnl
})

output$listersOnboarding <- renderGvis({
  listers <- gvisAreaChart(Activelisters, 
                           xvar = 'Date', yvar = 'Active listers', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#336699']"
                           ))              
  listers
})

output$replieshourOnboarding <- renderGvis({
  replieshour <- gvisAreaChart(Replieshour, 
                           xvar = 'Date', yvar = 'Replies', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#663366']"
                           ))              
  replieshour
})

output$nnlhourOnboarding <- renderGvis({
nnlhour <- gvisAreaChart(NNLhour, 
                               xvar = 'Date', yvar = 'NNL', options = list(
                                 legend = 'none',
                                 backgroundColor = "{fill:'transparent'}",
                                 colors = "['#990066']"
                               ))              
 nnlhour
})



}








 