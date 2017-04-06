# the body will contain four fluid rows:
# first row has the three key performance indicators
# second row will contain two graphs, a bar graph for sales revenue by region and quarter and a bar graph for prior and current year sales
# third fluid row will contain a line graph for %percent share, and an interactive bar graph instead of a pie chart
# a table with a drop down selector to see the data

# Key performance indicators using dynamic value boxes
frow1 <- fluidRow(
  valueBoxOutput("registeredUsers", width = 3)
  ,valueBoxOutput("registeredProfessionals", width = 3)
  ,valueBoxOutput("activeRequests", width = 2)
  ,valueBoxOutput("quotesSent", width = 2)
  ,valueBoxOutput("satisfiedRequests", width = 2)
)

# Sales by quarter, year
frow2 <- fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Some random stats"
    ,status = "primary"
    ,solidHeader = TRUE 
    ,collapsible = TRUE 
    ,plotOutput("salesQuartBar", height = "300px")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Another random stats"
    ,status = "primary"
    ,solidHeader = TRUE 
    ,collapsible = TRUE 
    ,plotOutput("salesYearBar", height = "300px")
  ) 
  
)

# ouput for the source info
frow5 <- fluidRow(
  infoBoxOutput("sourceBox", width = 8)
  ,infoBoxOutput("nameBox", width = 4)
)

#############################################################################
#output for the dashboardBody tabItems
tab_dashboard <-  tabItem(tabName = "dashboard", frow1,frow2, frow5)

#function to process the dashboard on the server
server_dashboard <- function(input, output) { 
  # fluid row 1, kpi 1: pieces sold
  output$activeRequests <- renderValueBox({
    valueBox(
      formatC(100, format="d", big.mark=',')
      ,"Active Requests"
      ,icon = icon("inbox")
      ,color = "green")
  })
  
  # fluid row 1, kpi 2: market share
  output$registeredUsers <- renderValueBox({
    valueBox(
      formatC(24562, format="d", big.mark=',')
      ,"Registered Users"
      ,icon = icon("users")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$registeredProfessionals <- renderValueBox({
    valueBox(
      formatC(1302, format="d", big.mark=',')
      ,"Registered Professionals"
      ,icon = icon("user-md")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$quotesSent <- renderValueBox({
    valueBox(
      formatC(202, format="d", big.mark=',')
      ,"Quotes Sent"
      ,icon = icon("eur")
      ,color = "green")
  })
  
  # fluid row 1, kpi 3: profit margin
  output$satisfiedRequests <- renderValueBox({
    valueBox(
      formatC(124, format="d", big.mark=',')
      ,"Satisfied Requests"
      ,icon = icon("thumbs-o-up")
      ,color = "green")
  })
  
  # fluid row 5: source of the data
  output$sourceBox <- renderInfoBox({
    infoBox(
      title = "Source"
      ,value = "Google Analytics, MixPanel and Fixly database"
      ,color = "purple"
      ,icon = icon("tachometer")
    )
  })
  
  # fluid row 5: source of the data
  output$nameBox <- renderInfoBox({
    infoBox(
      title = "Author"
      ,value = "Antonio Costa"
      ,color = "purple"
      ,icon = icon("code")
    )
  })
}