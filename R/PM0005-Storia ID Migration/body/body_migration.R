# #############################################################################
# # output to dashboardBody
# #############################################################################
# Key performance indicators using dynamic value boxes
row_migration <- fluidRow(
useShinyjs(),
     # first box for sales by quarter and region bar
     valueBoxOutput("dauBox", width = 3),
     valueBoxOutput("repliesBox", width = 3),
     valueBoxOutput("nnlBox", width = 3), 
     valueBoxOutput("conversionBox", width = 3),
     valueBoxOutput("activeadsBox", width = 3),
     valueBoxOutput("sourceBox", width = 3))
  
 


tab_main_users_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="DAU",id="dauId",value='dauVal',
             box(
               title = "Daily Active Users"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("dauOnboarding")
             )),
    tabPanel(title="Message Replies",id="repliesId",value='repliesVal',
             box(
               title = "Message Replies"
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
    tabPanel(title="Reply Conversion",id="conversionId",value='conversionVal',
             box(
               title = "Reply Conversion (by Adpage Visits)"
               ,status = "primary"
               ,solidHeader = FALSE
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("conversionOnboarding")
             )),
    tabPanel(title="Active Ads",id="activeadsId",value='activeadsVal',
             box(
               title = "Active Ads"
               ,status = "primary"
               ,solidHeader = FALSE
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("activeadsOnboarding")
             )),
   tabPanel(title="DAU by Source",id="sourceId",value='sourceVal',
            box(
               title = "DAU by Source"
               ,status = "primary"
               ,solidHeader = FALSE
                ,collapsible = FALSE
                ,width = 12
                ,htmlOutput("sourceOnboarding")
             ))

    
    
  )
    )
 
# #############################################################################
# # output to server function

tab_migration <- tabItem(tabName = "migration",row_migration,tab_main_users_line_evolution) 

# ##############################################################################

server_migration <- function(input, output, session) {
 
 # fluid row 1, kpi 1: DAU
  output$dauBox <- renderValueBox({
        valueBox("827", "DAU",
           icon = icon("user"),
       color = "orange")
 })
  
  # fluid row 1, kpi 2: DAU by Source
  output$sourceBox <- renderValueBox({
    valueBox("827","DAU by Source",
             icon = icon("users"),
             color = "blue")
  })
  
  
  # fluid row 1, kpi 3: Replies
  output$repliesBox <- renderValueBox({
    valueBox("3", "Message Replies",
             icon = icon("reply"),
             color = "yellow")
  })
  

  # fluid row 1, kpi 4: NNL
  output$nnlBox <- renderValueBox({
    valueBox("2 559 ",
             "NNL",
             icon = icon("sign-in"),
             color = "maroon")
 })
 
  #  fluid row 1, kpi 5: Active Ads 
 output$activeadsBox <- renderValueBox({
 valueBox("610 005",
        "Active Ads",
         icon = icon("building"),
        color = "teal")
})

  # fluid row 1, kpi 6: Conversion Rate
  output$conversionBox <- renderValueBox({
    valueBox("0.5%","Reply Conversion","(by Ad visits)",
             icon = icon("check-circle"),
             color = "olive")
  })


#    on click
#    # # on click of a tab1 valuebox
shinyjs::onclick('dauBox',expr={
     # move to tab2
updateTabsetPanel(session, "navbar", 'dauVal')
})

  #    # # on click of a tab2 valuebox
  shinyjs::onclick('sourceBox',expr={
    # move to tab2
   updateTabsetPanel(session, "navbar", 'sourceVal')
  })
  


#  # # on click of a tab3 valuebox
shinyjs::onclick('repliesBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'repliesVal')
})

#  # # on click of a tab4 valuebox
shinyjs::onclick('nnlBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'nnlVal')
})


#  # # on click of a tab5 valuebox
shinyjs::onclick('activeadsBox',expr={
  # move to tab2
  updateTabsetPanel(session, "navbar", 'activeadsVal')
})

# #  # # on click of a tab6 valuebox
shinyjs::onclick('conversionBox',expr={
#   # move to tab2
updateTabsetPanel(session, "navbar", 'conversionVal')
  })



#DAU plot 

output$dauOnboarding <- renderGvis({
  user <- gvisAreaChart(UsersStorTotal, 
                        xvar = 'Date', yvar = 'DAU', options = list(
                          legend = 'none',
                          backgroundColor = "{fill:'transparent'}",
                          colors = "['#ff6600']"
                        ))
  user
  
})

#DAU by Source plot 

output$sourceOnboarding <- renderGvis({
  source <- gvisAreaChart(UsersSource, 
                       xvar = 'Date', yvar = c('Organic Users','Direct Users','Paid Users'), options = list(
                         legend = 'none', 
#vAxes="[{viewWindowMode:'explicit',
#viewWindow:{min:0, max:370000}}]",
                         backgroundColor = "{fill:'transparent'}",
                         colors = "['#0066ff','#339999','#333399']"
                       ))              
  source
})

### Replies

output$repliesOnboarding <- renderGvis({
  replies <- gvisAreaChart(StoriaReplies, 
                       xvar = 'Date', yvar = 'Replies', options = list(
                         legend = 'none',
                         backgroundColor = "{fill:'transparent'}",
                         colors = "['#ffcc99']"
                       ))              
 replies
})
# 


### NNL 

output$nnlOnboarding <- renderGvis({
  nnl <- gvisAreaChart(NNL, 
                           xvar = 'Date', yvar = 'NNL', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#990066']"
                           ))              
  nnl
})

output$activeadsOnboarding <- renderGvis({
  activeads <- gvisAreaChart(Activeads,
                           xvar = 'Date', yvar = 'Active Ads', options = list(
                             legend = 'none',
                             backgroundColor = "{fill:'transparent'}",
                             colors = "['#33cccc']"
                           ))
  activeads
})


output$conversionOnboarding <- renderGvis({
conversion <- gvisAreaChart(ConversionRate,
                               xvar = 'Date', yvar = 'CR', options = list(
                                 legend = 'none',
                                 backgroundColor = "{fill:'transparent'}",
                                 colors = "['#339966']"
                               ))
 conversion
})


}








 