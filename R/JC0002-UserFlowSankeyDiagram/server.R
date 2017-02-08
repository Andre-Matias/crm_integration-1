library(shiny)
library(jsonlite)
library(reshape)
library(googleVis)
library(gsubfn)
library(stringr)

shinyServer(function(input, output) {
  
  ### Get the inputs ###
  
  start_date <- reactive(as.character(input$dates[1]))
  end_date <- reactive(as.character(input$dates[2]))
  event <- reactive(as.character(input$event))
  depth <- reactive(input$depth)
  nb_users <- reactive(input$nb_users)
  
  ### Create the API ###
  
  jqlquery <- reactive({
    paste0("https://",
           "3c3321ad060219024fcbcd906b1d19e3",
           "@mixpanel.com/api/2.0/jql?script=function%20main()%20%7B%0A%20%20return%20Events(%7B%0A%20%20%20%20from_date%3A%20%27",
           start_date(),
           "%27%2C%0A%20%20%20%20to_date%3A%20%20%20%27",
           end_date(),
           "%27%2C%0A%20%20%7D)%0A%20%20.groupByUser(function(flow%2C%20events)%20%7B%0A%20%20%20%20flow%20%3D%20flow%20%7C%7C%20%7B%20depth%3A%200%20%7D%3B%0A%20%20%20%20flow.current%20%3D%20flow.current%20%7C%7C%20flow%3B%0A%20%20%20%20for%20(var%20i%20%3D%200%3B%20i%20%3C%20events.length%3B%20i%2B%2B)%20%7B%0A%20%20%20%20%20%20var%20e%20%3D%20events%5Bi%5D%3B%0A%20%20%20%20%20%20if%20(flow.depth%20%3D%3D%3D%200%20%26%26%20e.name%20!%3D%20%22",
           event(),
           "%22)%20%7B%0A%20%20%20%20%20%20%20%20continue%3B%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20if%20(flow.depth%20%3D%3D%3D%20",
           depth(),
           ")%20%7B%0A%20%20%20%20%20%20%20%20return%20flow%3B%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20flow.depth%2B%2B%3B%0A%20%20%20%20%20%20flow.current%5Be.name%5D%20%3D%20flow.current%5Be.name%5D%20%7C%7C%20%7B%27count%27%3A%200%2C%27next%27%3A%20%7B%7D%7D%3B%0A%20%20%20%20%20%20flow.current%5Be.name%5D.count%2B%2B%3B%0A%20%20%20%20%20%20flow.current%20%3D%20flow.current%5Be.name%5D.next%3B%0A%20%20%20%20%7D%0A%20%20%20%20return%20flow%3B%0A%20%20%7D)%0A%20%20%20%20.map(function(item)%20%7B%0A%20%20%20%20delete%20item.value.depth%3B%0A%20%20%20%20delete%20item.value.current%3B%0A%20%20%20%20return%20item.value%3B%0A%20%20%7D)%0A%20%20.reduce(mixpanel.reducer.object_merge())%3B%0A%7D"
           )
  })
  
  ### User flow script ###
  
  userflow_table <- reactive({
    
      ## create table
    
    userflow <- fromJSON(jqlquery())
    userflow <- flatten(userflow)
    userflow <- melt(userflow)
    userflow <- userflow[-c(1),]
    rownames(userflow) <- 1:nrow(userflow)
    userflow <- rename(userflow, c("variable"="target_full"))
    userflow$target_full <- as.character(userflow$target_full)
    userflow$target_full = substr(userflow$target_full,1,nchar(userflow$target_full)-6)
    userflow$target_full = gsub(".next.",">", userflow$target_full)
    userflow$source_full <- lapply(userflow[,c("target_full")], function(x){substr(x, 1,  max(unlist(gregexpr('>', x)))-1)})
    userflow <- userflow[,c(3,1,2)]
    userflow$target_full <- as.factor(userflow$target_full)
    userflow$source_full <- as.factor(unlist(userflow$source_full))
    
      ## add count_source and count_target
    
    userflow$count_source <- str_count(userflow$source_full, ">")
    userflow$count_target <- str_count(userflow$target_full, ">")
    
      ## prefix
    
    userflow$prefix <- rownames(userflow)
    userflow$prefix <- with(userflow, userflow$prefix[ifelse(userflow$count_source == 0, TRUE, NA)])
    userflow$prefix <- as.integer(userflow$prefix)*10
    
      ## fill the NAs
    
    var_na <- NULL
    for(i in 1:nrow(userflow)){
      na_or_not <- is.na(userflow[i,c('prefix')])
      if (na_or_not == FALSE) {
        var_na <- userflow[i,c('prefix')]
      } else {
        userflow[i,c('prefix')] <- var_na
      }
    }
    
      ## create source_prefix and target_prefix
    
    userflow$source_prefix <- userflow$count_source + userflow$prefix
    userflow$target_prefix <- userflow$count_target + userflow$prefix
    
      ## put 0 as a prefix if source_count = 0
    
    for(i in 1:nrow(userflow)){
      var_0 <- userflow[i,c('count_source')]
      if (var_0 == 0) {
        userflow[i,c('source_prefix')] <- 0
      }
    }
    
      ## get the last event of the >
    
    userflow$source <- paste(
      userflow$source_prefix,
      sapply(strsplit(as.character(userflow$source_full), ">"),tail, 1),
      sep = "_")
    userflow$target <- paste(
      userflow$target_prefix,
      sapply(strsplit(as.character(userflow$target_full), ">"),tail, 1),
      sep = "_")
    
      ## reorder the columns
    
    userflow <- userflow[,c(9,10,3,1,2,4,5,6,7,8)]
    userflow$value <- (userflow$value)
    
      ## create table for the sankey
    
    sankeytable <- rename(data.frame(userflow$source, userflow$target, userflow$value),
                          c("userflow.source" = "source", "userflow.target" = "target", "userflow.value" = "users"))
    
      ## add the nb_users
    
    sankeytable <- sankeytable[(sankeytable[,3]>nb_users()),]
    
  })
  
  ### Sankey diagram ###
  
  output$sankey = renderGvis({
    chart <- userflow_table()
    mySankey <- gvisSankey(chart, from="source", 
                           to="target", weight="value",
                           options=list(
                             width=500,
                             height=300,
                             sankey="{
                              link:{colorMode:'gradient',color:['#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99', '#1f78b4', '#33a02c']},
                              node:{
                                nodePadding:20,
                                width: 10,
                                interactivity: true,
                                  label:{fontName:'Calibri', fontSize:14},
                                colors: ['#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99', '#1f78b4', '#33a02c']
                              }}"
                           ))
  mySankey
  })
  
  ### API debugger ###
  
  output$api = renderText({
    jqlquery()
  })
  
  ### Downloads ###
  
    ## Download the sankey table
  
  output$download_sankeydata = downloadHandler(
    filename = "sankey_data.csv",
    content = function(file) {write.csv(userflow_table(),file)}
  )
  
})