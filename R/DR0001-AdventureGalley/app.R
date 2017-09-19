library(shiny)
library(shinydashboard)

ui <- 
  dashboardPage(
  dashboardHeader(title = "Adventure Galley"),

# dashboardSidebar start ------------------------------------------------------

  dashboardSidebar(
      sidebarMenu(
        menuItem("Monetization", tabName = "monetization", icon = icon("money"),
                   menuItem('Posting Flow', tabName = 'postingflow', 
                            menuSubItem('Sub-Item Two', tabName = 'subItemTwo'))
                 )
      ),
      sidebarMenu(
        menuItem("Business Tool", tabName = "bt", icon = icon("wrench"))
      )
  ),
# dashboardSidebar end --------------------------------------------------------

# dashboardbody start ---------------------------------------------------------
  dashboardBody()
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { }

# Run the application 
shinyApp(ui = ui, server = server)

