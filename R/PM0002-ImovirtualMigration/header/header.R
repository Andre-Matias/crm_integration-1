# for the dashboard header, enter a title of "Global Sales Overview"
ddm_notifications <-dropdownMenu(type = "notifications",
                                 notificationItem(
                                   text = paste("Data last updated on ", "2017-06-03", "14:26"),
                                   icon("clock-o"),
                                   status = "success"
                                 )
)

# for the dashboard header, enter a title of "Global Sales Overview"
header <- dashboardHeader(title="Imovirtual Migration KPI",ddm_notifications)


