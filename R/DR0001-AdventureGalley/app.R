library(shiny)
library(shinydashboard)

ui <- 
  dashboardPage(
  dashboardHeader(title = "Adventure Galley"),

# dashboardSidebar start ------------------------------------------------------

  dashboardSidebar(
      sidebarMenu(
        menuItem("Monetization", tabName = 'tabMonetization', icon = icon('money'),
                   menuItem('Posting Flow', tabName = 'tabPostingFlow', 
                            menuSubItem('Drop Reasons', tabName = 'tabDropReasons'))
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

