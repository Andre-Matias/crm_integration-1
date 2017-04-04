tab_main <-fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Segment Description"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,dataTableOutput("MainDescription")
  )
  
  # second box for sales by year and region bar
  ,box(
   title = "Percent of Segments"
   ,status = "primary"
   ,solidHeader = TRUE
   ,collapsible = TRUE
   ,plotOutput("MainHeatMap", height = "300px")
  )
)

tab_total <-fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("totalProfByCat", height = "300px", click = "plotTotal_click")
  ) ,
  
  div(style="display:inline-block;", downloadButton('downloadTotalProf', 'Download'), style="float:right"),
  # first box for sales by quarter and region bar
  box(
    title = ""
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,dataTableOutput("totalProfTable")
  )
)

#tab_main_detail <- column(dataTableOutput('mytable3'), width = 3)

tab_gold <-fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("goldProfByCat", height = "300px", click = "plotGold_click")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users Onboarding"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("goldProfOnboarding", height = "300px")
  )
)

tab_gold_download <- fluidRow(
  div(style="display:inline-block;", downloadButton('downloadGoldProf', 'Download'), style="float:right")
)

tab_gold_table <-fluidRow(
  column(
    dataTableOutput(outputId = "goldProfTable"), width = 12)
)

tab_silver <- fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("silverProfByCat", height = "300px", click = "plotSilver_click")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users Onboarding"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("silverProfOnboarding", height = "300px")
  )
)

tab_silver_download <- fluidRow(
  div(style="display:inline-block;", downloadButton('downloadSilverProf', 'Download'), style="float:right")
)

tab_silver_table <- fluidRow(  
  column(
    dataTableOutput(outputId = "silverProfTable"), height = "150px", width = 12)
)

tab_bronze <- fluidRow(
  # first box for sales by quarter and region bar
  box(
    title = "Professional Users by Category"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("bronzeProfByCat", height = "300px", click = "plotBronze_click")
  )
  
  # second box for sales by year and region bar
  ,box(
    title = "Professional Users Onboarding"
    ,status = "primary"
    ,solidHeader = TRUE
    ,collapsible = TRUE
    ,plotOutput("bronzeProfOnboarding", height = "300px")
  )
)


tab_bronze_download <- fluidRow(
  div(style="display:inline-block;", downloadButton('downloadBronzeProf', 'Download'), style="float:right")
)

tab_bronze_table <- fluidRow(
  column(
    dataTableOutput(outputId = "bronzeProfTable"), height = "150px", width = 12)
)



frow_overlap1<- fluidRow(
  tabBox(
    #title = "Buckets",
    # The id lets us use input$tabset1 on the server to find the current tab
    id = "tab_buckets",
    width = 12,
    height = "800px",
    tabPanel("Main", tab_main,tab_total),
    tabPanel("Gold Bucket", tab_gold, tab_gold_download, tab_gold_table),
    tabPanel("Silver Bucket", tab_silver, tab_silver_download, tab_silver_table),
    tabPanel("Bronze Bucket", tab_bronze, tab_bronze_download, tab_bronze_table)
  )
)

#############################################################################
#output to dashboardBody
tab_overlap <- tabItem(tabName = "overlap", frow_overlap1)

#output to server function
server_overlap = function(input, output) {
  output$mytable3 = renderDataTable({
    iris
  }, options = list(lengthMenu = c(5, 30, 50), pageLength = 5))
  
  output$MainDescription <- renderDataTable({
    df_desc},
    options = list(paging = FALSE, Width = "50px", searching = FALSE)
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
  TotalProf <- subset(df_unpvot, variable %in% c("TOTAL","TOTALBOTH"), select = c(Service,CATEGORYID,CATEGORY,variable,value))
  
  TotalProfOrder <- subset(df_unpvot, variable %in% c("TOTAL"), select = CATEGORY)
  
  output$totalProfByCat <- renderPlot({
    ggplot(data = TotalProf,
           aes(x=CATEGORY, y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Users") +
      scale_x_discrete(limits=TotalProfOrder$CATEGORY) +
      xlab("Source") + theme(legend.position="bottom"
                             ,plot.title = element_text(size=15, face="bold"),
                             axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # fluid row Gold, graph 1: sales by region quarter bar graph
  goldProf <- subset(df_unpvot, variable %in% c("GOLD","GOLDBOTH"), select = c(Service,CATEGORYID,CATEGORY,variable,value))
  
  goldProfOrder <- subset(df_unpvot, variable %in% c("GOLD"), select = CATEGORY)
  
  output$goldProfByCat <- renderPlot({
    ggplot(data = goldProf,
           aes(x=CATEGORY, y=value, fill=variable)) +  
      scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_bar(stat="identity") + ylab("# Professional Users") +
      scale_x_discrete(limits=goldProfOrder$CATEGORY) +
      xlab("Source") + theme(legend.position="bottom"
                             ,plot.title = element_text(size=15, face="bold"),
                             axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold"))
  })
  
  # # fluid row Gold, graph 2: sales be region current/prior year
  goldProf_daily <- subset(df_unpvot_daily, variable %in% c("GOLD","GOLDBOTH"), select = c(CATEGORY,CATEGORYID,DIA,variable,value))
  output$goldProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    subsetGoldProfDaily <- as.data.frame(goldProf_daily[goldProf_daily$CATEGORYID == as.numeric(x_Numeric(input$plotGold_click)),])
    
    ggplot(data=subsetGoldProfDaily, aes(x=DIA, y=value, group=CATEGORY, colour=CATEGORY)) + scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_line() +
      geom_point()
  })
  
  
  # # fluid row Silver, graph 2: sales be region current/prior year
  silverProf <- subset(df_unpvot, variable %in% c("SILVER","SILVERBOTH"), select = c(Service,CATEGORYID,CATEGORY,variable,value))
  silverProfOrder <- subset(df_unpvot, variable %in% c("GOLD"), select = CATEGORY)
  output$silverProfByCat <- renderPlot({
    ggplot(data = silverProf,
           aes(x=factor(CATEGORY), y=value, fill=variable)) +  scale_fill_manual(values=c("#9999CC", "#66CC99")) +  
      geom_bar(stat="identity") + ylab("# Professional Users") + 
      scale_x_discrete(limits=silverProfOrder$CATEGORY) +
      xlab("Source") + theme(legend.position="bottom" 
                             ,plot.title = element_text(size=15, face="bold"),
                             axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold")) 
  })
  
  # # fluid row Silver, graph 2: sales be region current/prior year
  silverProf_daily <- subset(df_unpvot_daily, variable %in% c("SILVER","SILVERBOTH"), select = c(CATEGORY,CATEGORYID,DIA,variable,value))
  
  output$silverProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    subsetSilverProfDaily <- as.data.frame(silverProf_daily[silverProf_daily$CATEGORYID == as.numeric(x_Numeric(input$plotSilver_click)),])
    ggplot(data=subsetSilverProfDaily, aes(x=DIA, y=value, group=CATEGORY, colour=CATEGORY)) + scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_line() +
      geom_point()
  })
  
  
  # # fluid row bronze, graph 2: sales be region current/prior year
  bronzeProf <- subset(df_unpvot, variable %in% c("BRONZE","BRONZEBOTH"), select = c(Service,CATEGORYID,CATEGORY,variable,value))
  bronzeProfOrder <- subset(df_unpvot, variable %in% c("GOLD"), select = CATEGORY)
  #mdfr2 <- dcast(mdfr, id.vars = c("Service","CATEGORY"))
  output$bronzeProfByCat <- renderPlot({
    ggplot(data = bronzeProf,
           aes(x=factor(CATEGORY), y=value, fill=variable)) +  scale_fill_manual(values=c("#9999CC", "#66CC99")) +  
      geom_bar(stat="identity") + ylab("# Professional Users") +
      scale_x_discrete(limits=bronzeProfOrder$CATEGORY) + 
      xlab("Source") + theme(legend.position="bottom" 
                             ,plot.title = element_text(size=15, face="bold"),
                             axis.text.x = element_text(angle = 25, hjust = 1,size=10, face="bold")) 
  })
  
  # # fluid row Silver, graph 2: sales be region current/prior year
  bronzeProf_daily <- subset(df_unpvot_daily, variable %in% c("BRONZE","BRONZEBOTH"), select = c(CATEGORY,CATEGORYID,DIA,variable,value))
  
  output$bronzeProfOnboarding <- renderPlot({
    x_Numeric <- function(e) {
      if(is.null(e)) return(1)
      round(e$x, 0)
    } 
    subsetBronzeProfDaily <- as.data.frame(bronzeProf_daily[bronzeProf_daily$CATEGORYID == as.numeric(x_Numeric(input$plotBronze_click)),])
    ggplot(data=subsetBronzeProfDaily, aes(x=DIA, y=value, group=CATEGORY, colour=CATEGORY)) + scale_fill_manual(values=c("#9999CC", "#66CC99")) +
      geom_line() +
      geom_point()
  }) 
  
  output$downloadTotalProf <- downloadHandler(
    filename = function() { paste("input$date_range[1]", '.csv', sep='') },
    content = function(file) {
      write.csv(TotalProf, file)
    }
  )
  
  output$downloadGoldProf <- downloadHandler(
    filename = function() { paste("input$date_range[1]", '.csv', sep='') },
    content = function(file) {
      write.csv(goldProf, file)
    }
  )
  
  output$downloadSilverProf <- downloadHandler(
    filename = function() { paste("input$date_range[1]", '.csv', sep='') },
    content = function(file) {
      write.csv(silverProf, file)
    }
  )
  
  output$downloadBronzeProf <- downloadHandler(
    filename = function() { paste("input$date_range[1]", '.csv', sep='') },
    content = function(file) {
      write.csv(bronzeProf, file)
    }
  )
  
  x_Numeric <- function(e) {
    if(is.null(e)) return(1)
    round(e$x, 0)
  }
  
  output$goldProfTable   <- renderDataTable(as.data.frame(goldProf[goldProf$CATEGORYID == as.numeric(x_Numeric(input$plotGold_click)),]), options = list(pageLength=5))
  output$silverProfTable <- renderDataTable(as.data.frame(silverProf[silverProf$CATEGORYID == as.numeric(x_Numeric(input$plotSilver_click)),]), options = list(pageLength=5))
  output$bronzeProfTable <- renderDataTable(as.data.frame(bronzeProf[bronzeProf$CATEGORYID == as.numeric(x_Numeric(input$plotBronze_click)),]), options = list(pageLength=5))
  output$totalProfTable  <- renderDataTable(as.data.frame(TotalProf[TotalProf$CATEGORYID == as.numeric(x_Numeric(input$plotTotal_click)),]), options = list(pageLength=5, Width = "50px", searching = FALSE))
  
  
  output$info <- renderText({
    paste0("x=", input$plot_click$x, "\ny=", input$plot_click$y)
  })

}
