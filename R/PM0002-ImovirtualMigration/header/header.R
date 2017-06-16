# for the dashboard header, enter a title of "Global Sales Overview"
ddm_notifications <-dropdownMenu(type = "notifications",
                                 notificationItem(
                                   text = paste("Data last updated on ", "2017-06-16", "09:40"),
                                   icon("clock-o"),
                                   status = "success"
                                 )
)

# for the dashboard header, enter a title of "Global Sales Overview"
header <- dashboardHeader(title="Imovirtual Migration KPI",ddm_notifications)


