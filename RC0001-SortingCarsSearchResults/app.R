# Load up the shiny package.
library(shiny)
# Load up the Scales package.
library('scales')

#Load the file containing the DataFrame with Autovit data
load("ExibitionAutovit.RData")
#Load the file containing DataFrame with Otomot data
load("ExibitionOtomoto.RData")

source('ExibitionFunctions.R')


#Define the Shiny Server informations
server <- function(input, output) {
  #colSums (TotalAutovit[,c(2,3,4)])
  output$AutovitTotal <- renderDataTable({
    TotalExibitionAutovit(input)
  }, options = list(ordering=F,paging = FALSE,
                    searching = FALSE,
                    orderClasses = TRUE,
                    order = list(list(0, 'desc'))
  )
  )
  
  output$OtomotoTotal <- renderDataTable({
    TotalExibitionOtomoto(input)
  }, options = list(ordering=F,paging = FALSE,
                    searching = FALSE,
                    orderClasses = TRUE,
                    order = list(list(0, 'desc'))
  )
  )
  
  output$AutovitTable <- renderDataTable({
    ExibitionAutovit[ExibitionAutovit$Date >= input$date_range[1] & ExibitionAutovit$Date <= input$date_range[2], ]
  }, options = list(paging = FALSE,
                    searching = FALSE,
                    orderClasses = TRUE,
                    order = list(list(0, 'desc'))
  )
  )
  
  output$OtomotoTable <- renderDataTable({
    ExibitionOtomoto[ExibitionOtomoto$Date >= input$date_range[1] & ExibitionOtomoto$Date <= input$date_range[2], ]
  }, options = list(paging = FALSE,
                    searching = FALSE,
                    orderClasses = TRUE,
                    order = list(list(0, 'desc'))
  )
  )
}

#Define the UI Shiny Informations
ui <- fluidPage(
  #Title of the page
  titlePanel("Sorting Analysis"),
  helpText("Analysis of the amount of use of the sorting 
           function per Mileage and Power Engine in relation to total sortings"),
  sidebarPanel(
  
  
  sliderInput("date_range", "Choose Date Range:", min = as.POSIXlt("2016-12-12"),
              max = as.POSIXlt(AutovitExecutedDate-1),
              value = c(as.POSIXlt(AutovitExecutedDate-11),as.POSIXlt(AutovitExecutedDate-1)),
              timeFormat = "%Y-%m-%d", ticks = F, animate = F,width = '98%'),
  hr(),
  helpText("Source: Google Analytics"),
  h6("Author: Rodrigo de Caro")),
  
  mainPanel(
  tabsetPanel(id = "tabSelected",
              tabPanel("Autovit", 
                       h6("Date: ", AutovitExecutedDate),
                       dataTableOutput("AutovitTable"), 
                       dataTableOutput("AutovitTotal")
              ),
              tabPanel("Otomoto",
                       h6("Date: ", OtomotoExecutedDate),
                       dataTableOutput("OtomotoTable"),  
                       dataTableOutput("OtomotoTotal")
              )
  )
  ))


shinyApp(ui = ui, server = server)

