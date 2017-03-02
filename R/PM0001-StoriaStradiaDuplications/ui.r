##ui.R
library(shiny)

navbarPage(
  title = (""),
  tabPanel('Overview',titlePanel("Storia.IND and Stradia.ID Duplicated Ads"),
             mainPanel(
               h5("This report provides data about ad duplications for Storia Indonesia and Stradia India based on some variables."),
               br(),
               h5("To find ad duplicates we use the following variables for Storia: 
                  Same user id, same city id, same category id, same price and title similarity with at least 70%.
                  For Stradia we use the following variables:
                  Same user id, same brand, same model, same year, same mileage and description similarity with at least 70%."),
               br(),
               h5("In Storia and Stradia graph tabs, we have a plot with the relation between active ads and ad duplications, by day. 
                  Due performance and memory capacity reasons, we just consider a maximum of 30 days per plot. With that we can have an overall perspective regarding the evolution of duplicates."), 
               br(),
               h5("In Storia and Stradia tables you can find the current duplications (daily active) and you can look deep for the data using the variables that we used."),
               br(),
               h5("We need to consider that some users can cheat us with different prices, titles, description, mileages or even year so is expected that we might have more duplicates than this report shows."),
                  br(), 
                  br(),
                  h6("Date: 30 days for graphs, current day for tables"),
                  h6("Source: Database"),
                  h6("Author: Pedro Matos"))),  
  tabPanel('Storia Graph', plotOutput("duplicatesPlot")),   
  tabPanel('Storia Table', DT::dataTableOutput('ex1'))
  #tabPanel('Stradia Graph', plotOutput("duplicatesPlot2")),   
  #tabPanel('Stradia Table', DT::dataTableOutput('ex2'))
           
  
  
)




