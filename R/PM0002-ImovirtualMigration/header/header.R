# for the dashboard header, enter a title of "Global Sales Overview"
ddm_notifications <-dropdownMenu(type = "notifications",
                                 notificationItem(
                                   text = paste("Data last updated on ", "2017-06-10", "18:55"),
                                   icon("clock-o"),
                                   status = "success"
                                 )
)

# for the dashboard header, enter a title of "Global Sales Overview"
header <- dashboardHeader(title="Imovirtual Migration KPI",ddm_notifications)


