##################################################################
# MAIN TAB
##################################################################

tab_main <-fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Segmentation Criteria (OLX.pl) "
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,dataTableOutput("MainDescription")
    ,strong("Descriptions"),br()
    ,strong("Gold - "),em(df_desc[1,8]),br()
    ,strong("Silver - "),em(df_desc[2,8]),br()
    ,strong("Bronze - "),em(df_desc[3,8]),br()
    ,strong("Tin - "),em(df_desc[4,8]),br()
    ,strong("Drop-off - "),em(df_desc[5,8])
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users by Bucket (OLX.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("MainHeatMap", height = "300px")
  )
)

tab_total <-fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category (Overlap OLX.pl and Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("totalProfByCat", height = "300px", click = "plotTotal_click")
  ) ,
  
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users Additional Data (OLX.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,div(style="display:inline-block;", downloadButton('downloadTotalProf', 'Download'), style="float:right")
    ,dataTableOutput("totalProfTable")
  )
)

##################################################################
# GOLD TAB
##################################################################

tab_gold <-fluidRow(
  # first box for Professional Users by Category
  box(
    title = "Professional Users by Category (Overlap OLX.pl and Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,verbatimTextOutput("goldInfo")
    ,plotOutput("goldProfByCat", height = "300px", click = "plotGold_click")
  )
  
  # second box for Professional Users Onboarding
  ,box(
    title = "Professional Users Onboarding (Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,plotOutput("goldProfOnboarding", height = "300px")
  )
)

tab_gold_table <-fluidRow(
  box(
    title = "Professional Users Additional Data (OLX.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    , width = 12
    ,div(style="display:inline-block;", downloadButton('downloadGoldProf', 'Download'), style="float:right")
    ,dataTableOutput(outputId = "goldProfTable"))
)

##################################################################
# SILVER TAB
##################################################################

tab_silver <- fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category (Overlap OLX.pl and Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,verbatimTextOutput("silverInfo")
    ,plotOutput("silverProfByCat", height = "300px", click = "plotSilver_click")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users Onboarding (Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,plotOutput("silverProfOnboarding", height = "300px")
  )
)

tab_silver_table <-fluidRow(
  box(
    title = "Professional Users Additional Data (OLX.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    , width = 12
    ,div(style="display:inline-block;", downloadButton('downloadSilverProf', 'Download'), style="float:right")
    ,dataTableOutput(outputId = "silverProfTable")
  )
)

##################################################################
# BRONZE TAB
##################################################################


tab_bronze <- fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category (Overlap OLX.pl and Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,verbatimTextOutput("bronzeInfo")
    ,plotOutput("bronzeProfByCat", height = "300px", click = "plotBronze_click")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users Onboarding (Fixly.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,height = "420px"
    ,plotOutput("bronzeProfOnboarding", height = "300px")
  )
)

tab_bronze_table <- fluidRow(
  box(
    title = "Professional Users Additional Data (OLX.pl)"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    , width = 12
    ,div(style="display:inline-block;", downloadButton('downloadBronzeProf', 'Download'), style="float:right")
    ,dataTableOutput(outputId = "bronzeProfTable")
  )
)

##################################################################
# ALL
##################################################################

frow_overlap1<- fluidRow(
  tabBox(
    #title = "Buckets",
    # The id lets us use input$tabset1 on the server to find the current tab
    id = "tab_buckets",
    width = 12,
    height = "800px",
    tabPanel("Global",tab_main,tab_total),
    tabPanel("Gold Bucket", tab_gold, tab_gold_table),
    tabPanel("Silver Bucket", tab_silver, tab_silver_table),
    tabPanel("Bronze Bucket", tab_bronze, tab_bronze_table)
  )
)

#############################################################################
# output to dashboardBody
#############################################################################
tab_overlap <- tabItem(tabName = "overlap", frow_overlap1)

#############################################################################
# output to server function
#############################################################################

server_overlap = function(input, output) {
  df_desc_character <- df_desc
  df_desc_character$`# professionals` <- as.character(df_desc_character$`# professionals`)
  df_desc_character$`# active ads` <- as.character(df_desc_character$`# active ads`)
  df_desc_character$`vas generated revenue` <- as.character(df_desc_character$`vas generated revenue`)
  output$MainDescription <- renderDataTable(
    DT::datatable(df_desc_character,
                  class = c('compact', 'cell-border'),
                  options = list(dom = 'ft',paging = FALSE,ordering=FALSE, searching = FALSE, autoWidth = FALSE,
                                 rowCallback = JS('function(row, column, data) {$("td", row).css("text-align", "left");
                                                  }'),
                                 initComplete = JS(
                                   "function(settings, json) {",
                                   "$(this.api().table().header()).css({'background-color': '#9999CC ', 'color': '#000'});",
                                   #"$(this.api().table().header()).css({'text-align', 'right'});",
                                   "}"),
                                 columnDefs = list(list(visible=FALSE, targets=c(0,8)
                                 )
                                 )))
  )
  
  output$MainHeatMap <- renderPlot({
    treemap(totalUsersPerBucket, #Your data frame object
            index="label",  #A list of your categorical variables
            vSize = "count",  #This is your quantitative variable
            type="index", #Type sets the organization and color scheme of your treemap
            palette = "BuPu",  #Select your color palette from the RColorBrewer presets or make your own.
            title="", #Customize your title
            fontsize.title = 14 #Change the font size of the title
    )
  })
  
  # fluid row Gold, graph 1: sales by region quarter bar graph
  totalProf <- subset(df_unpvot, variable %in% c("total.olx","total.olx.and.fixly"), select = c(service,categoryid,category,variable,value))
  
  totalProf$variable <- factor(totalProf$variable, levels = c("total.olx.and.fixly","total.olx"),ordered = FALSE)
  
  output$totalProfByCat <- renderPlot({
    ggplot(data = totalProf,
           aes(x=reorder(category, categoryid), y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Userss") +
      geom_text(data=subset(totalProf,value>0), aes(label=value),  colour="black", position=position_dodge(width=0.9), vjust=-0.25, size=3, check_overlap = TRUE)+
      xlab("Ads Category (L2)") + theme(legend.position="bottom"
                                        ,plot.title = element_text(size=15, face="bold"),
                                        axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # fluid row Gold, graph 1: sales by region quarter bar graph
  goldProf <- subset(df_unpvot, variable %in% c("gold.olx","gold.olx.and.fixly"), select = c(service,categoryid,category,variable,value))
  
  goldProf$variable <- factor(goldProf$variable, levels = c("gold.olx","gold.olx.and.fixly"),ordered = TRUE)
  
  output$goldProfByCat <- renderPlot({
    ggplot(data = goldProf,
           aes(x=reorder(category, categoryid), y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Users") +
      geom_text(data=subset(goldProf,value>0), aes(label=value),  colour="black", position=position_dodge(width=0.9), vjust=-0.25, size=3, check_overlap = TRUE)+
      xlab("Ads Category (L2)") + theme(legend.position="bottom"
                                        ,plot.title = element_text(size=15, face="bold"),
                                        axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  goldProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "GOLD"))
  
  output$goldProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=goldProf_daily[goldProf_daily$categoryid == as.numeric(x_Numeric(input$plotGold_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  
  # # fluid row Silver, graph 2: sales be region current/prior year
  silverProf <- subset(df_unpvot, variable %in% c("silver.olx","silver.olx.and.fixly"), select = c(service,categoryid,category,variable,value))
  
  silverProf$variable <- factor(silverProf$variable, levels = c("silver.olx","silver.olx.and.fixly"),ordered = TRUE)
  
  output$silverProfByCat <- renderPlot({
    ggplot(data = silverProf,
           aes(x=reorder(category, categoryid), y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Users") +
      geom_text(data=subset(silverProf,value>0), aes(label=value),  colour="black", position=position_dodge(width=0.9), vjust=-0.25, size=3, check_overlap = TRUE)+
      xlab("Ads Category (L2)") + theme(legend.position="bottom"
                                        ,plot.title = element_text(size=15, face="bold"),
                                        axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  silverProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "SILVER"))
  
  output$silverProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=silverProf_daily[silverProf_daily$categoryid == as.numeric(x_Numeric(input$plotSilver_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  
  # # fluid row bronze, graph 2: sales be region current/prior year
  bronzeProf <- subset(df_unpvot, variable %in% c("bronze.olx","bronze.olx.and.fixly"), select = c(service,categoryid,category,variable,value))
  
  bronzeProf$variable <- factor(bronzeProf$variable, levels = c("bronze.olx","bronze.olx.and.fixly"),ordered = TRUE)
  
  output$bronzeProfByCat <- renderPlot({
    ggplot(data = bronzeProf,
           aes(x=reorder(category, categoryid), y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Users") +
      geom_text(data=subset(bronzeProf,value>0), aes(label=value),  colour="black", position=position_dodge(width=0.9), vjust=-0.25, size=3, check_overlap = TRUE)+
      xlab("Ads Category (L2)") + theme(legend.position="bottom"
                                        ,plot.title = element_text(size=15, face="bold"),
                                        axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # # fluid row Bronze, graph 2: sales be region current/prior year
  bronzeProf_daily <- as.data.frame(subset(df_teste_daily, bucket == "BRONZE"))
  
  output$bronzeProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    
    ggplot(data=bronzeProf_daily[bronzeProf_daily$categoryid == as.numeric(x_Numeric(input$plotBronze_click)),], aes(x=created_at, y=sum, group=category, shape=category))  +
      geom_line(colour="#66CC99", size=1) +
      geom_point(colour="#66CC99", size=2) +
      scale_shape_discrete(name  ="Ads Category (L2)")+
      geom_text(aes(label=sum),  colour="black", position=position_dodge(width=0.9), vjust=-1.25, size=3, check_overlap = TRUE) +
      ylab("# Registered Professional Users") +
      xlab("Registration Date") +
      scale_x_date(date_labels = "%b %d")
  })
  
  
  # # download buttons
  output$downloadTotalProf <- downloadHandler(
    filename = function() { paste("TotalSegmentation_filtered_", format(Sys.time(), "%Y%m%d%H%M%S"),'.csv', sep='') },
    content = function(file) {
      write.csv(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotTotal_click)),], file)
    }
  )
  
  output$downloadGoldProf <- downloadHandler(
    filename = function() { paste("GoldBucket_filtered_", format(Sys.time(), "%Y%m%d%H%M%S") , '.csv', sep='') },
    content = function(file) {
      write.csv(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotGold_click)) & df_prof_bucket$bucket=="GOLD",], file)
    }
  )
  
  output$downloadSilverProf <- downloadHandler(
    filename = function() { paste("SilverBucket_filtered_", format(Sys.time(), "%Y%m%d%H%M%S") , '.csv', sep='') },
    content = function(file) {
      write.csv(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotSilver_click)) & df_prof_bucket$bucket=="SILVER",], file)
    }
  )
  
  output$downloadBronzeProf <- downloadHandler(
    filename = function() { paste("BronzeBucket_filtered_", format(Sys.time(), "%Y%m%d%H%M%S") , '.csv', sep='') },
    content = function(file) {
      write.csv(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotBronze_click)) & df_prof_bucket$bucket=="BRONZE",], file)
    }
  )
  
  x_Numeric <- function(e) {
    if(is.null(e)) return(1)
    round(e$x, 0)
  }
  
  # # tables with professionals data
  output$goldProfTable   <- renderDataTable(as.data.frame(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotGold_click)) & df_prof_bucket$bucket=="GOLD",]), options = list(pageLength=5, columnDefs = list(list(visible=FALSE, targets=c(0,3,9)))))
  output$silverProfTable <- renderDataTable(as.data.frame(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotSilver_click)) & df_prof_bucket$bucket=="SILVER",]), options = list(pageLength=5, columnDefs = list(list(visible=FALSE, targets=c(0,3,9)))))
  output$bronzeProfTable   <- renderDataTable(as.data.frame(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotBronze_click)) & df_prof_bucket$bucket=="BRONZE",]), options = list(pageLength=5, columnDefs = list(list(visible=FALSE, targets=c(0,3,9)))))
  output$totalProfTable  <- renderDataTable(as.data.frame(df_prof_bucket[df_prof_bucket$categoryid == as.numeric(x_Numeric(input$plotTotal_click)),]), options = list(pageLength=3, columnDefs = list(list(visible=FALSE, targets=c(0,3,5,8,9)))))
  
  # # labels with info
  output$goldInfo = renderText("Please click on the bar chart to filter the gold bucket data")
  output$silverInfo = renderText("Please click on the bar chart to filter the silver bucket data")
  output$bronzeInfo = renderText("Please click on the bar chart to filter the bronze bucket data")
  }
