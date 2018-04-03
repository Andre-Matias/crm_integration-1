# I create (manually for the moment) a vector with special events
special_events <- c('logout','posting_form_click','advanced_search_click')

for (i in 1:nrow(verticals_reference)){
  # get the variables as character
  area <- as.character(verticals_reference[i,'area'])
  country <- as.character(verticals_reference[i,'country'])
  universe <- as.character(verticals_reference[i,'universe'])
  event <- as.character(verticals_reference[i,'event'])
  property <- as.character(verticals_reference[i,'property'])
  
  # I create vectors to get the properties per audit type
  permanent <- verticals_list[[area]][[country]][['properties']][['permanent']]
  loggeduser <- verticals_list[[area]][[country]][['properties']][['loggeduser']]
  categories <- verticals_list[[area]][[country]][['properties']][['categories']]
  location <- verticals_list[[area]][[country]][['properties']][['location']]
  shortlisted <- verticals_list[[area]][[country]][['properties']][['shortlisted']][[universe]][['properties']]
  
  # allocate the audit type
  if (property %in% permanent){
    verticals_reference[i,'audit_type'] <- 'permanent'
  }
  else if (property %in% loggeduser){
    verticals_reference[i,'audit_type'] <- 'loggeduser'
  }
  else if (property %in% categories){
    verticals_reference[i,'audit_type'] <- 'categories'
  }
  else if (property %in% location){
    verticals_reference[i,'audit_type'] <- 'location'
  }
  else if (property %in% shortlisted){
    verticals_reference[i,'audit_type'] <- 'shortlisted'
  }
  else {
    verticals_reference[i,'audit_type'] <- 'default'
  }
  
  ### when it requires a special audit
  # special events with touch_point_page
  if (event %in% special_events & property == 'touch_point_page') {
    verticals_reference[i,'audit_special'] <- 'dynamic_tpp'
  }
} # end for
