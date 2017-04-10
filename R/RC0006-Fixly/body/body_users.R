# the body will contain four fluid rows:
# first row has the three key performance indicators
# second row will contain two graphs, a bar graph for sales revenue by region and quarter and a bar graph for prior and current year sales
# third fluid row will contain a line graph for %percent share, and an interactive bar graph instead of a pie chart
# a table with a drop down selector to see the data

# Key performance indicators using dynamic value boxes
tab_main_users_box <-fluidRow(
  useShinyjs(),
  # first box for sales by quarter and region bar
  valueBoxOutput("mauBox", width = 2)
  ,valueBoxOutput("percentOfActiveUsersBox", width = 2)
  ,valueBoxOutput("numberOfRequestBox", width = 2)
  ,valueBoxOutput("usersXrequestPerActiveUsersBox", width = 2)
  ,valueBoxOutput("totalRequestsBox", width = 2)
  ,valueBoxOutput("usersRatesXStarsBox", width = 2)
  
)
# Key performance indicators using dynamic value boxes
tab_main_users_box_2 <-fluidRow(
  useShinyjs(),
  # first box for sales by quarter and region bar
  valueBoxOutput("bounceRateBox", width = 2)
  ,valueBoxOutput("sticknessBox", width = 2)
  ,valueBoxOutput("numberOfPostingUsersBox", width = 2)
  ,valueBoxOutput("satisfiedUsersBox", width = 2)
  ,valueBoxOutput("professionalRatingUsersBox", width = 2)
  
)

tab_main_users_line_evolution <- fluidRow(
  tabsetPanel(
    id = "navbar",
    tabPanel(title="Monthly Active Users",id="mauId",value='mauVal',
             plotOutput("mauOnboarding", height = "300px")),
    
    tabPanel(title="Bounce Rate",id="bounceRateId",value='bounceRateVal',
             plotOutput("bounceRateOnboarding", height = "300px")),
    
    tabPanel(title="Percent of Active Users",id="percentOfActiveUsersId",value='percentOfActiveUsersVal',
             plotOutput("percentOfActiveUsersOnboarding", height = "300px")),
    
    tabPanel(title="Stickness",id="sticknessId",value='sticknessVal',
             plotOutput("sticknessOnboarding", height = "300px")),
    
    tabPanel(title="Number of Request",id="numberOfRequestId",value='numberOfRequestVal',
             plotOutput("numberOfRequestOnboarding", height = "300px")),
    
    tabPanel(title="Number of Posting Users",id="numberOfPostingUsersId",value='numberOfPostingUsersVal',
             plotOutput("numberOfPostingUsersOnboarding", height = "300px")),
    
    tabPanel(title="Users X Request Per Active Users",id="usersXrequestPerActiveUsersId",value='usersXrequestPerActiveUsersVal',
             plotOutput("usersXrequestPerActiveUsersOnboarding", height = "300px")),
  
    tabPanel(title="Satisfied Users",id="satifiedUsersId",value='satisfiedUsersVal',
             plotOutput("satisfiedUsersOnboarding", height = "300px")),
    
    tabPanel(title="Total Request",id="totalRequestsId",value='totalRequestsVal',
             plotOutput("totalRequestsOnboarding", height = "300px")),
    
    tabPanel(title="Professional Rating Users",id="professionalRatingUsersId",value='professionalRatingUsersVal',
             plotOutput("professionalRatingUsersOnboarding", height = "300px")),
    
    tabPanel(title="Users Rates x Stars",id="usersRatesXStarsId",value='usersRatesXStarsVal',
             plotOutput("usersRatesXStarsOnboarding", height = "300px"))
  )
)

#############################################################################
#output for the dashboardBody tabItems
tab_users <- tabItem(tabName = "users",tab_main_users_box,tab_main_users_box_2,tab_main_users_line_evolution)

#function to process the dashboard on the server
server_users <- function(input, output, session) { 
  
  
  # fluid row 1, kpi 2: market share
  output$mauBox <- renderValueBox({
    valueBox("1.1 K",
             "MAU",
             icon = icon("list"),
             color = "aqua")
  })
  
  # fluid row 1, kpi 2: market share
  output$bounceRateBox <- renderValueBox({
    valueBox("25%",
             "Bounce Rate",
             icon = icon("list"),
             color = "aqua")
  })
  
  
  # fluid row 1, kpi 1: pieces sold
  output$percentOfActiveUsersBox <- renderValueBox({
    valueBox("45%"
             ,"Active Users"
             ,icon = icon("inbox")
             ,color = "green")
  })
  
  # fluid row 1, kpi 1: pieces sold
  output$sticknessBox <- renderValueBox({
    valueBox("30%"
             ,"Stickness"
             ,icon = icon("inbox")
             ,color = "green")
  })
  
  # fluid row 1, kpi 2: market share
  output$numberOfRequestBox <- renderValueBox({
    valueBox(
      formatC(24562, format="d", big.mark=',')
      ,"Number of Request"
      ,icon = icon("users")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$numberOfPostingUsersBox <- renderValueBox({
    valueBox(
      formatC(1302, format="d", big.mark=',')
      ,"Posting Users"
      ,icon = icon("user-md")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$usersXrequestPerActiveUsersBox <- renderValueBox({
    valueBox(
      formatC(202, format="d", big.mark=',')
      ,"Users with X Request"
      ,icon = icon("eur")
      ,color = "orange")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$satisfiedUsersBox <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Satisfied Users"
      ,icon = icon("thumbs-o-up")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$totalRequestsBox <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Total Request"
      ,icon = icon("thumbs-o-up")
      ,color = "orange")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$professionalRatingUsersBox <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Professional Rating"
      ,icon = icon("thumbs-o-up")
      ,color = "yellow")
  })
  
  
  # fluid row 1, kpi 3: profit margin
  output$usersRatesXStarsBox <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Rate X Stars"
      ,icon = icon("thumbs-o-up")
      ,color = "yellow")
  })
  
  #####################
  # on click of a tab1 valuebox
  shinyjs::onclick('mauBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'mauVal')
  })
  
  # on click of a tab1 valuebox
  shinyjs::onclick('bounceRateBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'bounceRateVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('percentOfActiveUsersBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'percentOfActiveUsersVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('sticknessBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'sticknessVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('numberOfRequestBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'numberOfRequestVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('numberOfPostingUsersBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'numberOfPostingUsersVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('usersXrequestPerActiveUsersBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'usersXrequestPerActiveUsersVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('satisfiedUsersBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'satisfiedUsersVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('totalRequestsBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'totalRequestsVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('professionalRatingUsersBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'professionalRatingUsersVal')
  })
  
  # on click of a tab2 valuebox
  shinyjs::onclick('usersRatesXStarsBox',expr={
    # move to tab2
    updateTabsetPanel(session, "navbar", 'usersRatesXStarsVal')
  })
  
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  goldProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "GOLD"))
  
  output$mauOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$bounceRateOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$percentOfActiveUsersOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$sticknessOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$numberOfRequestOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$numberOfPostingUsersOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$usersXrequestPerActiveUsersOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$satisfiedUsersOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$totalRequestsOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$professionalRatingUsersOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  output$usersRatesXStarsOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
}



