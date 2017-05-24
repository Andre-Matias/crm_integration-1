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
     valueBoxOutput("repliesBox", width = 3))
  
 


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
        valueBox("79 694", "DAU",
           icon = icon("user"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: Active ads
  output$adsBox <- renderValueBox({
    valueBox("302 565",
             "Active Ads",
             icon = icon("building-o"),
             color = "aqua")
  })
  
  # fluid row 1, kpi 3: Revenue
  output$revenueBox <- renderValueBox({
    valueBox("10 112",
             "Revenue",
             icon = icon("money"),
             color = "olive")
  })
  
  # fluid row 1, kpi 4: Replies
  output$repliesBox <- renderValueBox({
    valueBox("3 459",
             "Replies",
             icon = icon("reply"),
             color = "purple")
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
                         legend = 'none', vAxes="[{viewWindowMode:'explicit',
			                    viewWindow:{min:0, max:370000}}]",
                         backgroundColor = "{fill:'transparent'}",
                         colors = "['#66ccff']"
                       ))              
  ads
})

#revenue plot 

output$revenueOnboarding <- renderGvis({
  revenue <- gvisAreaChart(Imorevenueold, 
                           xvar = 'Date', yvar = 'Revenue', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#336633']"
                           ))              
  revenue
})

#Replies plot

output$repliesOnboarding <- renderGvis({
  replies <- gvisAreaChart(Imorepliesold, 
                           xvar = 'Date', yvar = 'Replies', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#663366']"
                           ))              
  replies
})

}



#336633
#663366




 