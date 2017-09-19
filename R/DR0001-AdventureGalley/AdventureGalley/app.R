library(shiny)
library(shinydashboard)

ui <- 
  dashboardPage(
  dashboardHeader(),
  dashboardSidebar(
    dashboardSidebar(
      sidebarMenu(
        menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
        menuItem("Widgets", tabName = "widgets", icon = icon("th"))
      )
  )
  ),
# dashboardbody start ---------------------------------------------------------
  dashboardBody()
# dashboardbody end -----------------------------------------------------------
)
server <- function(input, output) { }

# Run the application 
shinyApp(ui = ui, server = server)

