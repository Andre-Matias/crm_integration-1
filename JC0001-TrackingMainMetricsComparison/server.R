library(shiny)
library(jsonlite)
library(plyr)
library(googleVis)
shinyServer(function(input, output) {
  
  ### site ref table ###
  site_name <- c("otomotopl","otodompl","olxpl")
  site_mpcode <- c("ed06fd545816f0ef5c79f4936e603870","f11909a90782f605aef692025f648546","3c3321ad060219024fcbcd906b1d19e3")
  site_aticode <- c("574113","574115","574113")
  site_codes <- data.frame(site_name,site_mpcode,site_aticode)
  site_codes$site_name <- as.character(site_codes$site_name)
  site_codes$site_mpcode <- as.character(site_codes$site_mpcode)
  site_codes$site_aticode <- as.character(site_codes$site_aticode)
  site_choice_ati <- reactive({
    site_codes$site_aticode[site_codes$site_name == as.character(input$sitecode)]
    })
  site_choice_mp <- reactive({
    site_codes$site_mpcode[site_codes$site_name == as.character(input$sitecode)]
    })
  
  ### dates, platforms & credentials ###
  start_date <- reactive(as.character(input$dates[1]))
  end_date <- reactive(as.character(input$dates[2]))
  platform <- reactive(as.character(input$platforms))
  aticredentials <- reactive("jeremy.castan%40olx.com:Cavenaghi-33")
  
  ### api calls ###
  jqlquery_dau <- reactive({paste0("https://",
                              site_choice_mp(),
                              "@mixpanel.com/api/2.0/jql?script=",
                              "function%20main()%20%7B%0A%20%20return%20Events(%7B%0A%20%20%20%20from_date%3A%20%27",
                              start_date(),
                              "%27%2C%0A%20%20%20%20to_date%3A%20%20%20%27",
                              end_date(),
                              "%27%2C%0A%09event_selectors%3A%20%5B%7Bselector%3A%27properties%5B%22platform%22%5D%20%3D%3D%20%22",
                              platform(),
                              "%22%27%7D%5D%0A%20%20%7D)%0A.groupByUser(%5BgetDay%5D%2C%0A%20%20%20%20function()%20%7Breturn%201%7D%0A%20%20)%0A.groupBy(%5B%22key.1%22%5D%2Cmixpanel.reducer.count())%0A.map(function(col)%7B%0A%20%20%20return%20%7B%0A%20%20%20%20%20%271%27%20%3A%20col.key%5B0%5D%2C%0A%20%20%20%20%20%272%27%20%3A%20col.value%0A%20%20%20%7D%3B%0A%20%7D)%3B%0A%7D%0Afunction%20getDay(event)%20%7B%0A%20%20return%20(new%20Date(event.time)).toISOString().split(%27T%27)%5B0%5D%3B%7D"
  )})
  jqlquery_pv <- reactive({paste0("https://",
                              site_choice_mp(),
                              "@mixpanel.com/api/2.0/jql?script=",
                              "function%20main()%20%7B%0A%20%20return%20Events(%7B%0A%20%20%20%20from_date%3A%20%27",
                              start_date(),
                              "%27%2C%0A%20%20%20%20to_date%3A%20%20%20%27",
                              end_date(),
                              "%27%2C%0A%20%20%20%20event_selectors%3A%20%5B%0A%20%20%20%20%20%20%7Bselector%3A%27properties%5B%22platform%22%5D%20%3D%3D%20%22",
                              platform(),
                              "%22%27%7D%2C%0A%20%20%20%20%20%20%7Bselector%3A%27properties%5B%22event_type%22%5D%20%3D%3D%20%22pv%22%27%7D%0A%20%20%20%20%20%20%5D%0A%20%20%7D)%0A%20%20.groupBy(%5BgetDay%5D%2C%20mixpanel.reducer.count()).map(function(col)%7B%0A%20%20%20return%20%7B%0A%20%20%20%20%20%271%27%20%3A%20col.key%5B0%5D%2C%0A%20%20%20%20%20%272%27%20%3A%20col.value%0A%20%20%20%7D%3B%0A%20%7D)%3B%7D%0Afunction%20getDay(event)%20%7B%0A%20%20return%20(new%20Date(event.time)).toISOString().split(%27T%27)%5B0%5D%3B%7D"
  )})
  jqlquery_au <- reactive({paste0("https://",
                                  site_choice_mp(),
                                  "@mixpanel.com/api/2.0/jql?script=",
                                  "function%20main()%20%7B%0A%20%20return%20Events(%7B%0A%20%20%20%20from_date%3A%20%27",
                                  start_date(),
                                  "%27%2C%0A%20%20%20%20to_date%3A%20%20%20%27",
                                  end_date(),
                                  "%27%2C%0A%09event_selectors%3A%20%5B%7Bselector%3A%27properties%5B%22platform%22%5D%20%3D%3D%20%22",
                                  platform(),
                                  "%22%27%7D%5D%0A%20%20%7D)%0A.groupByUser(function()%20%7Breturn%201%20%7D)%0A%20%20.groupBy(%5Bfunction(row)%20%7B%20return%20row.key.slice(1)%20%7D%5D%2C%0A%20%20%20%20%20%20mixpanel.reducer.count()%0A%20%20)%3B%0A%7D"
  )})
  atiquery_dau <- reactive({paste0("https://",
                              aticredentials(),
                              "@apirest.atinternet-solutions.com/data/v2/json/getData?",
                              "&columns={d_time_date,m_visitors}&sort={d_time_date}&filter={cd_platfv2:{$eq:'",
                              platform(),
                              "'}}&space={s:",
                              site_choice_ati(),
                              "}&period={D:{start:'",
                              start_date(),
                              "',end:'",
                              end_date(),
                              "'}}&max-results=10000&page-num=1"
  )})
  atiquery_pv <- reactive({paste0("https://",
                                   aticredentials(),
                                   "@apirest.atinternet-solutions.com/data/v2/json/getData?",
                                   "&columns={d_time_date,m_page_views}&sort={d_time_date}&filter={cd_platfv2:{$eq:'",
                                   platform(),
                                   "'}}&space={s:",
                                   site_choice_ati(),
                                   "}&period={D:{start:'",
                                   start_date(),
                                   "',end:'",
                                   end_date(),
                                   "'}}&max-results=10000&page-num=1"
  )})
  atiquery_au <- reactive({paste0("https://",
                                            aticredentials(),
                                            "@apirest.atinternet-solutions.com/data/v2/json/getData?",
                                            "&columns={CM_39076}&sort={-CM_39076}&filter={cd_platfv2:{$eq:'",
                                            platform(),
                                            "'}}&space={s:",
                                            site_choice_ati(),
                                            "}&period={D:{start:'",
                                            start_date(),
                                            "',end:'",
                                            end_date(),
                                            "'}}&max-results=10000&page-num=1"
    )})
  
  ### create single tables ###
  mixpanel_dau <- reactive({
    jql <- fromJSON(jqlquery_dau())
    jql <- as.data.frame(jql)
    jql <- rename(jql, c("1"="date","2"="dau mixpanel"))
  })
  ati_dau <- reactive({
    ati <- fromJSON(atiquery_dau())
    ati <- as.data.frame(ati$DataFeed$Rows)
    ati <- rename(ati, c("d_time_date"="date","m_visitors"="dau ati"))
  })
  mixpanel_pv <- reactive({
    jql <- fromJSON(jqlquery_pv())
    jql <- as.data.frame(jql)
    jql <- rename(jql, c("1"="date","2"="pv mixpanel"))
  })
  ati_pv <- reactive({
    ati <- fromJSON(atiquery_pv())
    ati <- as.data.frame(ati$DataFeed$Rows)
    ati <- rename(ati, c("d_time_date"="date","m_page_views"="pv ati"))
  })
  mixpanel_au <- reactive({
    jql <- fromJSON(jqlquery_au())
    jql <- as.data.frame(jql)
    jql <- rename(jql, c("1"="date","2"="mp_au"))
    jql <- jql$mp_au
  })
  ati_au <- reactive({
    ati <- fromJSON(atiquery_au())
    ati <- as.data.frame(ati$DataFeed$Rows)
    ati <- ati$m_vu
  })
  finaltable <- reactive({
    Reduce(merge,list(mixpanel_dau(),ati_dau(),mixpanel_pv(),ati_pv()))
    })
  
  ### chart output ###
  output$activeusers = renderGvis({
    
  })
  output$chart_ati_pv = renderGvis({
    chart <- ati_pv()
    myLine <- gvisLineChart(chart, options = list(
      #trendlines = "{0:{type:'linear'}}"
      legend = 'none',
      hAxis = "{textPosition : 'none'}",
      vAxis = "{format:'###,###', minValue: 0}"
      #curveType = 'function'
      ))
    myLine
  })
  output$chart_mp_pv = renderGvis({
      chart <- mixpanel_pv()
    myLine <- gvisLineChart(chart, options = list(
      legend='none',
      hAxis ="{textPosition : 'none'}",
      vAxis = "{format:'###,###', minValue: 0}"
      #curveType = 'function'
      ))
    myLine
  })
  output$chart_mp_dau = renderGvis({
    chart <- mixpanel_dau()
    myLine <- gvisLineChart(chart, options = list(
      #trendlines = "{0:{type:'linear'}}"
      legend = 'none',
      hAxis = "{textPosition : 'none'}",
      vAxis = "{format:'###,###', minValue: 0}",
      backgroundColor = '#F5F5F5'
      #curveType = 'function'
    ))
    myLine
  })
  output$chart_ati_dau = renderGvis({
    chart <- ati_dau()
    myLine <- gvisLineChart(chart, options = list(
      #trendlines = "{0:{type:'linear'}}"
      legend = 'none',
      hAxis = "{textPosition : 'none'}",
      vAxis = "{format:'###,###', minValue: 0}",
      backgroundColor = '#F5F5F5'
      #curveType = 'function'
    ))
    myLine
  })
  
  ### download csv ###
  output$downloadcsv = downloadHandler(
    filename = "file.csv",
    content = function(file) {write.csv(finaltable(),file)}
  )
  
  ### debugger ###
  output$debug = renderUI({
    siteid <- paste("ATI site ID:",site_choice_ati())
    mixpaneltoken <- paste("Mixpanel token:",site_choice_mp())
    HTML(paste(siteid,mixpaneltoken, sep = '<br/>'))
  })
  output$api_mp_pv = renderText({
    jqlquery_pv()
  })
  output$api_mp_dau = renderText({
    jqlquery_dau()
  })
  output$api_mp_au = renderText({
    jqlquery_au()
  })
  output$api_ati_pv = renderText({
    atiquery_pv()
  })
  output$api_ati_dau = renderText({
    atiquery_dau()
  })
  output$api_ati_au = renderText({
    atiquery_au()
  })
})