# load the shinydashboard package
library(shiny)
library(shinydashboard)
library(shinyjs)
library(googleVis)

# library(ggplot2)
# library(plyr)
# library(treemap)
# library(DT)
# library(reshape2)
# library(RPostgreSQL)


#source the components of the app
source(file="data/data.R")
source(file="header/header.R")
source(file="sidebar/sidebar.R")
source(file="body/body.R")

# create the user interface for the dashboard
# all shiny dashboards have the same three basic layout elements:
# a header
# a sidebar
# a body

ui <- dashboardPage(header, sidebar, body, skin="black")

# create the server functions for the dashboard  
server <- function(input, output, session) { 
server_migration(input, output, session)
}

# render the dashboard as a shiny app
shinyApp(ui, server)
