# for the dashboard header, enter a title of "Global Sales Overview"
ddm_notifications <-dropdownMenu(type = "notifications",
                                 notificationItem(
                                   text = paste("Segmentation updated on ", df_desc[1,8]),
                                   icon("clock-o"),
                                   status = "success"
                                 ),
                                 notificationItem(
                                   text = paste("Dashboards updated on ", df_desc[1,8]),
                                   icon("clock-o"),
                                   status = "success"
                                 )
)


header <- dashboardHeader(title="Fixly.pl", ddm_notifications)
#header$children[[2]]$children <-  tags$a(href='http://mycompanyishere.com',
#                                   tags$img(src='olx.png',height='60',width='200'))

