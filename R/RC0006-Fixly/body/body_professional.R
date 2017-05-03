# the body will contain four fluid rows:
# first row has the three key performance indicators
# second row will contain two graphs, a bar graph for sales revenue by region and quarter and a bar graph for prior and current year sales
# third fluid row will contain a line graph for %percent share, and an interactive bar graph instead of a pie chart
# a table with a drop down selector to see the data

# Key performance indicators using dynamic value boxes
tab_main_professionals_box <-fluidRow(
  useShinyjs(),
  # first box for sales by quarter and region bar
  valueBoxOutput("mauBoxProf", width = 2)
  ,valueBoxOutput("percentOfActiveUsersBoxProf", width = 2)
  ,valueBoxOutput("bouncePaymentBoxProf", width = 2)
  ,valueBoxOutput("ApprovedQuotesPerActiveBoxProf", width = 2)
  ,valueBoxOutput("avgNumberOfQuotesBoxProf", width = 2)
  ,valueBoxOutput("avgRatingUsersBoxProf", width = 2)
  
)
# Key performance indicators using dynamic value boxes
tab_main_professionals_box_2 <-fluidRow(
  useShinyjs(),
  # first box for sales by quarter and region bar
  valueBoxOutput("bounceRateBoxProf", width = 2)
  ,valueBoxOutput("sticknessBoxProf", width = 2)
  ,valueBoxOutput("quotesBoxProf", width = 2)
  ,valueBoxOutput("ApprovedQuotesBoxProf", width = 2)
  ,valueBoxOutput("ratedProfessionals123StarsBoxProf", width = 2)
  ,valueBoxOutput("ratedProfessionals45StarsBoxProf", width = 2)
  
)

tab_main_professionals_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="Daily Active Users",id="mauIdProf",value='mauValProf',
             box(
               title = "Daily Active Users"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("mauOnboardingProf")
             )),
    tabPanel(title="Bounce Rate",id="bounceRateIdProf",value='bounceRateValProf',
             box(
               title = "Daily Bounce Rate"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("bounceRateOnboardingProf")
             )),
    tabPanel(title="Registered Professionals",id="percentOfActiveUsersIdProf",value='percentOfActiveUsersValProf',
             fluidRow(
               box(
                 title = "Daily Registered Professionals"
                 ,status = "primary"
                 ,solidHeader = FALSE 
                 ,collapsible = FALSE
                 ,width = 8
                 ,htmlOutput("percentOfActiveUsersOnboardingProf")
               ),
               infoBoxOutput("percentOfActiveUsersOnboardingBoxProf", width = 4),
               infoBoxOutput("bouncesRegistrationPage", width = 4)
             ),
             fluidRow(
               box(
                 title = "Registered Professionals by Category"
                 ,status = "primary"
                 ,solidHeader = FALSE 
                 ,collapsible = FALSE
                 ,width = 6
                 ,htmlOutput("registeredUsersPerCategory")
               ),
               box(
                 title = "Registered Professionals by City"
                 ,status = "primary"
                 ,solidHeader = FALSE 
                 ,collapsible = FALSE
                 ,width = 6
                 ,htmlOutput("registeredUsersPerCity")
               )
             )),
    
    tabPanel(title="Stickiness",id="sticknessIdProf",value='sticknessValProf',
             box(
               title = "Daily Stickiness"
               ,status = "primary"
               ,solidHeader = FALSE 
               ,collapsible = FALSE
               ,width = 12
               ,htmlOutput("sticknessOnboardingProf")
             ))
    # tabPanel(title="Bounce Payment",id="bouncePaymentIdProf",value='bouncePaymentValProf',
    #          htmlOutput("bouncePaymentOnboardingProf")),
    # 
    # tabPanel(title="Quotes",id="quotesIdProf",value='quotesValProf',
    #          htmlOutput("quotesOnboardingProf")),
    # 
    # tabPanel(title="Approved Quotes Per Active",id="ApprovedQuotesPerActiveIdProf",value='ApprovedQuotesPerActiveValProf',
    #          htmlOutput("ApprovedQuotesPerActiveOnboardingProf")),
    # 
    # tabPanel(title="Approved Quotes",id="ApprovedQuotesIdProf",value='ApprovedQuotesValProf',
    #          htmlOutput("ApprovedQuotesOnboardingProf")),
    # 
    # tabPanel(title="Average Number of Quotes",id="avgNumberOfQuotesIdProf",value='avgNumberOfQuotesValProf',
    #          htmlOutput("avgNumberOfQuotesOnboardingProf")),
    # 
    # tabPanel(title="Rated Professionals X Stars",id="ratedProfessionalsXStarsIdProf",value='ratedProfessionalsXStarsValProf',
    #          htmlOutput("ratedProfessionalsXStarsOnboardingProf")),
    # 
    # tabPanel(title="Average Rating Users",id="avgRatingUsersIdProf",value='avgRatingUsersValProf',
    #          htmlOutput("avgRatingUsersOnboardingProf"))
  )
)

#############################################################################
#output for the dashboardBody tabItems
tab_professional <- tabItem(tabName = "professionals",tab_main_professionals_box,tab_main_professionals_box_2,tab_main_professionals_line_evolution)

#function to process the dashboard on the server
server_professional <- function(input, output, session) { 
  
  
  # fluid row 1, kpi 2: market share
  output$mauBoxProf <- renderValueBox({
    valueBox(box_mau,
             paste("Monthly Active Users -",current_month),
             icon = icon("line-chart"),
             color = "aqua")
  })
  
  # fluid row 1, kpi 2: market share
  output$bounceRateBoxProf <- renderValueBox({
    valueBox(box_bounce_rate,
             paste("Bounce Rate -",current_month),
             icon = icon("sign-out"),
             color = "aqua")
  })
  
  
  # fluid row 1, kpi 1: pieces sold
  output$percentOfActiveUsersBoxProf <- renderValueBox({
    valueBox(box_registered_pros
             ,"Registered Professionals"
             ,icon = icon("wrench")
             ,color = "green")
  })
  
  # fluid row 1, kpi 1: pieces sold
  output$sticknessBoxProf <- renderValueBox({
    valueBox(box_stickiness
             ,"Stickiness"
             ,icon = icon("users")
             ,color = "green")
  })
  
  # fluid row 1, kpi 2: market share
  output$bouncePaymentBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Bounce Rate on Payment Form"
      ,icon = icon("thumbs-down")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$quotesBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Quotes"
      ,icon = icon("money")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$ApprovedQuotesPerActiveBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Pros with Approved Quotes"
      ,icon = icon("wrench")
      ,color = "orange")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$ApprovedQuotesBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Approved Quotes"
      ,icon = icon("money")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$avgNumberOfQuotesBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Quotes until One Gets Approved"
      ,icon = icon("money")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$ratedProfessionals45StarsBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Pros Rated with 4 or 5 Stars"
      ,icon = icon("wrench")
      ,color = "yellow")
  })
  
  output$ratedProfessionals123StarsBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Pros Rated with 1,2 or 3 Stars"
      ,icon = icon("wrench")
      ,color = "yellow")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$avgRatingUsersBoxProf <- renderValueBox({
    valueBox(
      dash
      ,"Average User Rating"
      ,icon = icon("star")
      ,color = "yellow")
  })
  
  
  
  # value box percentage of registered pros
  output$percentOfActiveUsersOnboardingBoxProf <- renderValueBox({
    infoBox(
      "Registered professionals over Entering Users"
      ,formatC(box_registered_pros_mau, format="d", big.mark=',')
      ,icon = icon("wrench")
      ,color = "green"
    )
  })
  
  # value box on bouces on registration page
  output$bouncesRegistrationPage <- renderValueBox({
    infoBox(
      "Bounce Rate on Registration Page"
      ,formatC(box_registration_bounce_rate, format="d", big.mark=',')
      ,icon = icon("thumbs-down")
      ,color = "green"
    )
  })
  
  #####################
  # on click of a tab1 valuebox
  shinyjs::onclick('mauBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'mauValProf')
  })
  
  # on click of a tab1 valuebox
  shinyjs::onclick('bounceRateBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'bounceRateValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('percentOfActiveUsersBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'percentOfActiveUsersValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('sticknessBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'sticknessValProf')
  })
  
  # # on click of a tab2 valuebox
  # shinyjs::onclick('bouncePaymentBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'bouncePaymentValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('quotesBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'quotesValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('ApprovedQuotesPerActiveBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'ApprovedQuotesPerActiveValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('ApprovedQuotesBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'ApprovedQuotesValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('avgNumberOfQuotesBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'avgNumberOfQuotesValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('ratedProfessionalsXStarsBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'ratedProfessionalsXStarsValProf')
  # })
  # 
  # # on click of a tab2 valuebox
  # shinyjs::onclick('avgRatingUsersBoxProf',expr={
  #   # move to tab2
  #   updateTabsetPanel(session, "navbar", 'avgRatingUsersValProf')
  # })
  
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  goldProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "GOLD"))
  
  output$mauOnboardingProf <- renderGvis({
    chart <- gvisLineChart(df_dailyDB, xvar = 'date', yvar = 'dau', options = list(
      legend = 'none',
      backgroundColor = "{fill:'transparent'}",
      vAxis = "{gridlines:{color: '#ECF0F5'}, format: '#'}",
      hAxis = "{gridlines:{color: 'transparent'}}",
      colors = "['#0D737B']"
    ))
    chart
  })
  
  output$bounceRateOnboardingProf <- renderGvis({
    chart <- gvisLineChart(df_dailyDB, xvar = 'date', yvar = 'bounce_rate', options = list(
      legend = 'none',
      backgroundColor = "{fill:'transparent'}",
      vAxis = "{gridlines:{color: '#ECF0F5'}, format: 'percent'}",
      hAxis = "{gridlines:{color: 'transparent'}}",
      colors = "['#00A65A']"
    ))
    chart
  })
  
  output$percentOfActiveUsersOnboardingProf <- renderGvis({
    i<- 2
    while (i < nrow(df_dailyDB)+1) {
      #look up stuff using data from the row
      #write stuff to the file
      if(df_dailyDB[i,7]==0){
        df_dailyDB[i,7] <- df_dailyDB[i-1,7]
      }
      i<- i+1
    }
    chart <- gvisLineChart(df_dailyDB, xvar = 'date', yvar = 'registered_professionals', options = list(
      legend = 'none',
      backgroundColor = "{fill:'transparent'}",
      vAxis = "{gridlines:{color: '#ECF0F5'}}",
      hAxis = "{gridlines:{color: 'transparent'}}",
      colors = "['#00A65A']"
    ))
    chart
  })
  
  output$registeredUsersPerCategory <- renderGvis({
    chart <- gvisBarChart(chart_prosPerL1cat, xvar = 'l1cat', yvar = c('count','count.html.tooltip'), options=list(
      legend = 'none',
      tooltip="{isHtml:'true',trigger:'selection'}",
      chartArea= "{left:150}",
      height=347
      ))
    chart
  })
  
  output$registeredUsersPerCity <- renderGvis({
    chart <- gvisGeoChart(df_citiesDB, locationvar = 'city_desc', colorvar = 'count',options=list(
      region = 'PL',
      displayMode='markers',
      resolution= 'provinces',
      colorAxis="{colors:['#F39C12','#00A65A']}",
      backgroundColor='#4D9EB2'
    ))
    chart
  })
  
  output$sticknessOnboardingProf <- renderGvis({
    chart <- gvisLineChart(df_dailyDB, xvar = 'date', yvar = 'stickiness', options = list(
      legend = 'none',
      backgroundColor = "{fill:'transparent'}",
      vAxis = "{gridlines:{color: '#ECF0F5'}}",
      hAxis = "{gridlines:{color: 'transparent'}}",
      colors = "['#0D737B']"
    ))
    chart
  })
  
  
}



