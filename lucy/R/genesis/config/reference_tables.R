#########
### VERTICALS
#########

# empty table that will be our reference
verticals_reference <- data.frame()

# loop starts
for (m in matrices_verticals){
  table <- eval(parse(text = m)) %>% gs_read(ws = 'matrix')
  business <- strsplit(m,'_')[[1]][2]
  area <- strsplit(m,'_')[[1]][3]
  universe <- strsplit(m,'_')[[1]][4]
  
  # setting the right conditions according to the context
  if (business == 'verticals'){
    countries <- names(verticals_list[[area]])
    platform <- c('dsk','rwd','and','ios')
  }
  
  for (c in countries){
    
    for (p in platform){
      
      # get events available in each platform and vectorize them
      events <- table %>% filter(eval(parse(text = p)) == 1) %>% select(`Event name`)
      events <- events[[1]]
      
      if (length(events) > 0){
        
        for (e in events){
          
          # filter on the event and select the columns which have an info inside
          properties <- table %>% filter(`Event name` == e) %>% select(-1,-2,-3,-4,-5,-6)
          properties <- colnames(properties)[colSums(is.na(properties)) == FALSE]
          # select the format of those properties
          properties_format <- as.character(slice(table %>% select(properties),1))
          # select the event_type value of the selected event
          event_type <- as.character(table %>% filter(`Event name` == e) %>% select(event_type))
          
          # then create the tmp table
          tmptable <- data.frame(
            business = business,
            area = area,
            country = c,
            universe = universe,
            platform = p,
            event = e,
            event_type = event_type,
            property = properties,
            format = properties_format
          )
          
          # add this tmp table to the final verticals_reference table
          verticals_reference <- rbind(verticals_reference,tmptable)
        } # end for events
        
      } # end if length(events)
      
    } # end for platform
    
  } # end for countries
  
} # end for matrices

