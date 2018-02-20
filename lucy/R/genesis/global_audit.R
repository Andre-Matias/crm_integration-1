### CONFIGURATION
#########
area <- c('cars','realestate')
country <- c('poland','romania')
universe <- c('adpage')
platform <- c('dsk','rwd')
platform_mixpanel <- c('desktop','rwd')

# Mixpanel audit

for (ar in area){
  for (co in country){
    for (pl in platform){
      for (un in universe){
        
        # create the scope of our audit
        audit_scope <- list(
          business = 'verticals',
          area = ar,
          country = co,
          platform = pl,
          platform_mixpanel = platform_mixpanel[match(pl,platform)],
          universe = un
        )
        
        # credentials mixpanel
        mp_credits <- mixpanelCreateAccount(queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][['credentials']][['project_name']],
                                            token = queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][['credentials']][['token']],
                                            secret = queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][['credentials']][['secret']],
                                            key = queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][['credentials']][['key']])
        
        # get tables from DB
        db_tables <- verticals_list[[audit_scope$area]][[audit_scope$country]][['db']]
        
        # filter the reference table on the dedicated scope
        audit_table <- verticals_reference %>% filter(
          business == audit_scope[['business']]
          & area == audit_scope[['area']]
          & country == audit_scope[['country']]
          & platform == audit_scope[['platform']]
          & universe == audit_scope[['universe']]
        )
        
        
        ### SETTING THE LIST 
        #########
        
        # get the audit types to run in a vector and make an audit list
        audit_type <- unique(audit_table[['audit_type']])
        audits <- list()
        
        # fill the audit list
        for (audit in audit_type){
          audits[[audit]] <- list()
          events <- unique(audit_table %>% filter(audit_type == audit) %>% select(event))[['event']]
          properties <- unique(audit_table %>% filter(audit_type == audit) %>% select(property))[['property']]
          audits[[audit]][['events']] <- events
          audits[[audit]][['properties']] <- properties
          audits[[audit]][['events_jql']] <- paste0("['",paste(events,collapse = "','"),"']")
          audits[[audit]][['properties_jql']] <- paste0("['name','",paste(paste0('properties.',properties),collapse = "','"),"']")
        }
        
        ### Querying the JQLs
        #########
        
        ### categories
        jql <- paste0(
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['categories']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          audits[['categories']][['properties_jql']],
          ", mixpanel.reducer.count())}"
        )
        
        auditedjql_categories <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event',as.character(audits[['categories']][['properties']]),'value'))
        
        ### permanent
        jql <- paste0(
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['permanent']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          audits[['permanent']][['properties_jql']],
          ", mixpanel.reducer.count())}"
        )
        
        auditedjql_permanent <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event',as.character(audits[['permanent']][['properties']]),'value'))
        
        ### location
        jql <- paste0(
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['location']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          audits[['location']][['properties_jql']],
          ", mixpanel.reducer.count())}"
        )
        auditedjql_location <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event',as.character(audits[['location']][['properties']]),'value'))
        
        ### loggeduser
        
        # remove the user id from the list of properties to get raw
        #properties_loggeduser <- audits[['loggeduser']][['properties']][-which(test == 'user_id')]
        #properties_loggeduser <- paste0("['name','",paste(paste0('properties.',properties_loggeduser),collapse = "','"),"',user_id_na,user_id_format]")
        properties_loggeduser <- "['name','properties.user_status','properties.business_status',user_id_na,user_id_format]"
        jql <- paste0(
          "function user_id_format(item){return _.isNumber(item.properties.user_id);}",
          "function user_id_na(item){return _.isUndefined(item.properties.user_id)}",
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['loggeduser']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          properties_loggeduser,
          ", mixpanel.reducer.count())}"
        )
        auditedjql_loggeduser <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event','user_status','business_status','user_id_na','user_id_format','value'))
        
        
        ### shortlisted
        jql <- paste0(
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['shortlisted']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          audits[['shortlisted']][['properties_jql']],
          ", mixpanel.reducer.count())}"
        )
        auditedjql_shortlisted <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event',as.character(audits[['shortlisted']][['properties']]),'value'))
        
        
        ### default
        
        ## make the functions
        
        # first I get the unique combinations of properties and formats
        default_properties <- unique(audit_table %>% filter(audit_type == 'default') %>% select(property,format))
        
        # then I apply the getJqlFunction to the column function
        default_properties$functions <- getJqlFunctions(default_properties$property,default_properties$format)
        
        # and I collapse this column
        jql_functions <- paste(default_properties[['functions']], collapse = '')
        
        # make the groupby
        jql_na <- paste0(default_properties[['property']],'_na')
        jql_format <- paste0(default_properties[['property']],'_format')
        jql_functions_list <- c(as.character(jql_na),as.character(jql_format))
        jql_functions_list_jql <- paste0("['name','properties.cat_l1_id',",paste(jql_functions_list, collapse = ','),"]")
        
        jql <- paste0(
          jql_functions,
          queries_list[['mixpanel']][['main']],
          queries_list[['mixpanel']][['html']],
          queries_list[['mixpanel']][['dsk']],
          queries_list[['mixpanel']][[audit_scope$area]][[audit_scope$country]][[audit_scope$universe]],
          ".filter(e=>_.contains(",
          audits[['default']][['events_jql']],
          ",e.name)===true)",
          ".groupBy(",
          jql_functions_list_jql,
          ", mixpanel.reducer.count())}"
        )
        auditedjql_default <- mixpanelJQLQuery(mp_credits,jql,columnNames = c('event','cat_l1_id',as.character(jql_functions_list),'value'))
        
        ### AUDITS
        #########
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### permanent
        #~~~~~~~~~~~~~~~~~~
        
        # platform
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'platform') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_permanent, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['platform']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(platform) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['platform']][['value']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(platform) == FALSE
            & platform != audit_scope$platform_mixpanel
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # event_type
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'event_type') %>% select(event,event_type,property)
        merged_table <- merge(filtered_matrix,auditedjql_permanent, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['event_type']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(event_type) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['event_type']][['value']] <-
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(event_type) == FALSE
            & event_type.x != event_type.y
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### logged user
        #~~~~~~~~~~~~~~~~~~
        
        # user_status
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'user_status') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_loggeduser, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['user_status']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(user_status) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['user_status']][['value']] <-
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(user_status) == FALSE
            & strsplit(user_status,'_')[[1]][1] %in% c('logged','unlogged') == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # business_status
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'business_status') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_loggeduser, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['business_status']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(business_status)
            & grepl('logged_',user_status)
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['business_status']][['value']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(business_status) == FALSE
            & grepl('logged_',user_status)
            & business_status %in% c('private','business') == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['business_status']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) 
            & is.na(business_status) == FALSE
            & user_status == 'unlogged'
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        
        # user_id
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'user_id') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_loggeduser, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['user_id']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE
            & user_id_na == TRUE 
            & grepl('logged_',user_status)
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['user_id']][['value']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE
            & user_id_na == FALSE 
            & grepl('logged_',user_status)
            & user_id_format == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['user_id']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE
            & user_id_na == FALSE 
            & user_status == 'unlogged'
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### location
        #~~~~~~~~~~~~~~~~~~
        
        # region_id
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'region_id') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_location, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['region_id']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(region_id) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['region_id']][['value']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & region_id %in% db_tables$location$regions$region_id == FALSE
            & is.na(region_id) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['region_id']][['noise']] <- 
          merged_table %>% filter(
            is.na(property)
            & is.na(region_id) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # region_name
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'region_name') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_location, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['region_name']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(region_name) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['region_name']][['noise']] <- 
          merged_table %>% filter(
            is.na(property)
            & is.na(region_name) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # city_id
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'city_id') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_location, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['city_id']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(city_id) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['city_id']][['value']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & city_id %in% db_tables$location$cities$city_id == FALSE
            & is.na(city_id) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['city_id']][['noise']] <- 
          merged_table %>% filter(
            is.na(property)
            & is.na(city_id) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        # city_name
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'city_name') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_location, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['city_name']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(city_name) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['city_name']][['noise']] <- 
          merged_table %>% filter(
            is.na(property)
            & is.na(city_name) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### categories
        #~~~~~~~~~~~~~~~~~~
        
        # cat_l1_id
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'cat_l1_id') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_categories, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_id']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(cat_l1_id) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_id']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) 
            & is.na(cat_l1_id) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_id']][['value']] <-
          merged_table %>% filter(
            is.na(property) == FALSE 
            & cat_l1_id %in% db_tables$categories$l1_cat$l1_id == FALSE
            & cat_l1_id != 161 # parts which are not in yamato yet
            & is.na(cat_l1_id) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # cat_l1_name
        #~~~~~~~~~~~~
        
        filtered_matrix <- audit_table %>% filter(property == 'cat_l1_name') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_categories, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_name']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(cat_l1_name) 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_name']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) 
            & is.na(cat_l1_name) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l1_name']][['value']] <-
          merged_table %>% filter(
            is.na(property) == FALSE 
            & cat_l1_name %in% db_tables$categories$l1_cat$l1_name == FALSE
            & cat_l1_name != 'parts' # parts which are not in yamato yet
            & is.na(cat_l1_name) == FALSE
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # cat_l2_id
        #~~~~~~~~~~~~
        filtered_matrix <- audit_table %>% filter(property == 'cat_l2_id') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_categories, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_id']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(cat_l2_id)
            & cat_l1_id %in% db_tables$categories$l1_parent_l2_id
            & cat_l1_id != 161 # parts which are not in yamato yet
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_id']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) 
            & is.na(cat_l2_id) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_id']][['value']] <- 
          merged_table %>% filter(
            is.na(property)  == FALSE
            & cat_l2_id %in% db_tables$categories$l2_cat$l2_id == FALSE
            & is.na(cat_l2_id) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        # cat_l2_name
        #~~~~~~~~~~~~
        filtered_matrix <- audit_table %>% filter(property == 'cat_l2_name') %>% select(event,property)
        merged_table <- merge(filtered_matrix,auditedjql_categories, by='event',all=TRUE)
        
        # check NAs
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_name']][['na']] <- 
          merged_table %>% filter(
            is.na(property) == FALSE 
            & is.na(cat_l2_name)
            & cat_l1_id %in% db_tables$categories$l1_parent_l2_id
            & cat_l1_id != 161 # parts which are not in yamato yet
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check noise
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_name']][['noise']] <- 
          merged_table %>% filter(
            is.na(property) 
            & is.na(cat_l2_name) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        # check value
        audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][['cat_l2_name']][['value']] <- 
          merged_table %>% filter(
            is.na(property)  == FALSE
            & cat_l2_name %in% db_tables$categories$l2_cat$l2_name == FALSE
            & is.na(cat_l2_name) == FALSE 
            & is.na(value) == FALSE) %>% select(event,value)
        
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### shortlisted
        #~~~~~~~~~~~~~~~~~~
        
        # adding the right vector of properties in the variable
        properties <- verticals_list[[audit_scope$area]][[audit_scope$country]][['properties']][['shortlisted']][[audit_scope$universe]]
        
        # run the same audit for properties there
        for(prop in properties$properties) {
          filtered_matrix <- audit_table %>% filter(property == prop) %>% select(event,property)
          merged_table <- merge(filtered_matrix,auditedjql_shortlisted, by='event',all=TRUE)
          
          # check NAs
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['na']] <- 
            merged_table %>% filter(
              is.na(property) == FALSE 
              & is.na(eval(parse(text = prop)))
              & is.na(value) == FALSE) %>% select(event,value)
          
          # check value
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['value']] <- 
            merged_table %>% filter(
              is.na(property) == FALSE 
              & is.na(eval(parse(text = prop))) == FALSE
              & eval(parse(text = prop)) %in% properties[[prop]] == FALSE
              & is.na(value) == FALSE) %>% select(event,value)
          
          # check noise
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['noise']] <- 
            merged_table %>% filter(
              is.na(property)
              & is.na(eval(parse(text = prop))) == FALSE
              & is.na(value) == FALSE) %>% select(event,value)
        }
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ### default
        #~~~~~~~~~~~~~~~~~~
        
        # get the default properties
        properties <- default_properties[['property']]
        
        for (prop in properties) {
          filtered_matrix <- audit_table %>% filter(property == prop) %>% select(event,property)
          merged_table <- merge(filtered_matrix,auditedjql_default, by='event',all=TRUE)
          prop_na <- paste0(prop,'_na')
          prop_format <- paste0(prop,'_format')
          
          # check NAs
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['na']] <- 
            merged_table %>% filter(
              is.na(property) == FALSE 
              & eval(parse(text = prop_na)) == TRUE
              & is.na(value) == FALSE) %>% select(event,value)
          
          # check format
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['format']] <- 
            merged_table %>% filter(
              is.na(property) == FALSE 
              & eval(parse(text = prop_format)) == FALSE
              & eval(parse(text = prop_na)) == FALSE
              & is.na(value) == FALSE) %>% select(event,value)
          
          # check noise
          audits___verticals_cars[[audit_scope$country]][[audit_scope$platform]][[audit_scope$universe]][[prop]][['format']] <- 
            merged_table %>% filter(
              is.na(property)
              & eval(parse(text = prop_na)) == FALSE
              & is.na(value) == FALSE) %>% select(event,value)
        }
        
        
      } # end for universe
    } # end for platform
  } # end for country
} # end for area

