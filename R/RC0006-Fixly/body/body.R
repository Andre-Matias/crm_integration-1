source(file="body/body_dashboard.R")
source(file="body/body_professional.R")
source(file="body/body_users.R")
source(file="body/body_overlap.R")

# combine the three fluid rows to make the body
body <- dashboardBody(
  # add css
  tags$head(
    tags$link(rel = 'stylesheet', type = 'text/css', href='style.css')
  ),
  #add google maps js
  tags$head(tags$script(src='https://maps.googleapis.com/maps/api/js?key=AIzaSyBIECL9yUPvqs3WUzMHC-GI1vDTMkt649c&callback=initMap')),
  #tabs
  div(style = 'overflow-y: scroll; overflow-x: hidden',
      tabItems(
        tab_dashboard,
        tab_professional,
        tab_users,
        tab_overlap
      )
  )
)