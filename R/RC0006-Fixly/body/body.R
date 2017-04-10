source(file="body/body_dashboard.R")
source(file="body/body_professional.R")
source(file="body/body_users.R")
source(file="body/body_overlap.R")

# combine the three fluid rows to make the body
body <- dashboardBody(
  div(style = 'overflow-y: scroll; overflow-x: hidden',
      tabItems(
        tab_dashboard,
        tab_professional,
        tab_users,
        tab_overlap
      )
  )
)