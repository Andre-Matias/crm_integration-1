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
  ,valueBoxOutput("aprovedQuotesPerActiveBoxProf", width = 2)
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
  ,valueBoxOutput("aprovedQuotesBoxProf", width = 2)
  ,valueBoxOutput("ratedProfessionalsXStarsBoxProf", width = 2)
  
)

tab_main_professionals_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="Monthly Active Users",id="mauIdProf",value='mauValProf',
             plotOutput("mauOnboardingProf", height = "300px")),
    
    tabPanel(title="Bounce Rate",id="bounceRateIdProf",value='bounceRateValProf',
             plotOutput("bounceRateOnboardingProf", height = "300px")),
    
    tabPanel(title="Percent of Active Users",id="percentOfActiveUsersIdProf",value='percentOfActiveUsersValProf',
             plotOutput("percentOfActiveUsersOnboardingProf", height = "300px")),
    
    tabPanel(title="Stickness",id="sticknessIdProf",value='sticknessValProf',
             htmlOutput("sticknessOnboardingProf")),
    
    tabPanel(title="Bounce Payment",id="bouncePaymentIdProf",value='bouncePaymentValProf',
             plotOutput("bouncePaymentOnboardingProf", height = "300px")),
    
    tabPanel(title="Quotes",id="quotesIdProf",value='quotesValProf',
             plotOutput("quotesOnboardingProf", height = "300px")),
    
    tabPanel(title="Aproved Quotes Per Active",id="aprovedQuotesPerActiveIdProf",value='aprovedQuotesPerActiveValProf',
             plotOutput("aprovedQuotesPerActiveOnboardingProf", height = "300px")),
  
    tabPanel(title="Aproved Quotes",id="aprovedQuotesIdProf",value='aprovedQuotesValProf',
             plotOutput("aprovedQuotesOnboardingProf", height = "300px")),
    
    tabPanel(title="Average Number of Quotes",id="avgNumberOfQuotesIdProf",value='avgNumberOfQuotesValProf',
             plotOutput("avgNumberOfQuotesOnboardingProf", height = "300px")),
    
    tabPanel(title="Rated Professionals X Stars",id="ratedProfessionalsXStarsIdProf",value='ratedProfessionalsXStarsValProf',
             plotOutput("ratedProfessionalsXStarsOnboardingProf", height = "300px")),
    
    tabPanel(title="Average Rating Users",id="avgRatingUsersIdProf",value='avgRatingUsersValProf',
             plotOutput("avgRatingUsersOnboardingProf", height = "300px"))
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
             "MAU Professionals",
             icon = icon("list"),
             color = "aqua")
  })
  
  # fluid row 1, kpi 2: market share
  output$bounceRateBoxProf <- renderValueBox({
    valueBox(box_bounce_rate,
             "Bounce Rate",
             icon = icon("list"),
             color = "aqua")
  })
  
  
  # fluid row 1, kpi 1: pieces sold
  output$percentOfActiveUsersBoxProf <- renderValueBox({
    valueBox("45%"
             ,"Active Users"
             ,icon = icon("inbox")
             ,color = "green")
  })
  
  # fluid row 1, kpi 1: pieces sold
  output$sticknessBoxProf <- renderValueBox({
    valueBox(box_stickiness
             ,"Stickness"
             ,icon = icon("inbox")
             ,color = "green")
  })
  
  # fluid row 1, kpi 2: market share
  output$bouncePaymentBoxProf <- renderValueBox({
    valueBox(
      formatC(24562, format="d", big.mark=',')
      ,"Bounce Payment"
      ,icon = icon("users")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$quotesBoxProf <- renderValueBox({
    valueBox(
      formatC(1302, format="d", big.mark=',')
      ,"Quotes"
      ,icon = icon("user-md")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$aprovedQuotesPerActiveBoxProf <- renderValueBox({
    valueBox(
      formatC(202, format="d", big.mark=',')
      ,"Aproved Quotes Per Active"
      ,icon = icon("eur")
      ,color = "orange")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$aprovedQuotesBoxProf <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Aproved Quotes"
      ,icon = icon("thumbs-o-up")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$avgNumberOfQuotesBoxProf <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Avg Number of Quotes"
      ,icon = icon("thumbs-o-up")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$ratedProfessionalsXStarsBoxProf <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Rated Professionals"
      ,icon = icon("thumbs-o-up")
      ,color = "yellow")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$avgRatingUsersBoxProf <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Avg Rating Users"
      ,icon = icon("thumbs-o-up")
      ,color = "yellow")
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
  
  # on click of a tab2 valuebox
  shinyjs::onclick('bouncePaymentBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'bouncePaymentValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('quotesBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'quotesValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('aprovedQuotesPerActiveBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'aprovedQuotesPerActiveValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('aprovedQuotesBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'aprovedQuotesValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('avgNumberOfQuotesBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'avgNumberOfQuotesValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('ratedProfessionalsXStarsBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'ratedProfessionalsXStarsValProf')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('avgRatingUsersBoxProf',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'avgRatingUsersValProf')
  })
  
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  goldProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "GOLD"))
  
  output$mauOnboardingProf <- renderPlot({
    ggplot(data=df_mau, aes(x=yearmonth, y=users, group=1)) +
      geom_line(color='red')+
      geom_point(color='red')
  })
  
  output$bounceRateOnboardingProf <- renderPlot({
    ggplot(data=df_traffic, aes(x=date, y=bounce_rate, group=1)) +
      geom_line(color='red')+
      geom_point(color='red')
  })
  
  output$percentOfActiveUsersOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$sticknessOnboardingProf <- renderGvis({
    chart <- gvisLineChart(df_traffic, xvar = 'date', yvar = 'stickiness', options = list(
      legend = 'none',
      backgroundColor = "{fill:'transparent'}",
      colors = "['#0D737B']"
    ))
    chart
  })
  
  output$bouncePaymentOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$quotesOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$aprovedQuotesPerActiveOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$aprovedQuotesOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$avgNumberOfQuotesOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$ratedProfessionalsXStarsOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$avgRatingUsersOnboardingProf <- renderPlot({
    ggplot(data=goldProf_daily, aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
}



