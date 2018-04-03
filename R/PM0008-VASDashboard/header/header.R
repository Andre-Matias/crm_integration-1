# for the dashboard header, enter a title of "Global Sales Overview"
ddm_notifications <-dropdownMenu(type = "notifications",
                                 notificationItem(
                                   text = paste("Data last updated on ", "2018-04-02", "18:05"),
                                   icon("clock-o"),
                                   status = "success"
                                 )
)

# for the dashboard header, enter a title of "Global Sales Overview"
header <- dashboardHeader(title="VAS Dashboard",ddm_notifications)


