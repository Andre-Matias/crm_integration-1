##sidebar <- dashboardSidebar(disable = TRUE)

sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
    menuItem("Professionals", icon = icon("user-md"), tabName = "professionals",
             badgeLabel = "new", badgeColor = "green"),
    menuItem("Users", icon = icon("users"), tabName = "users",
             badgeLabel = "new", badgeColor = "green"),
    menuItem("OLX Overlap", icon = icon("clone"), tabName = "overlap",
             badgeLabel = "new", badgeColor = "green")
  )
)