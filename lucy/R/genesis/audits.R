### CONFIGURATION
#########

# create the scope of our audit
audit_scope <- list(
  business = 'verticals',
  area = 'cars',
  country = 'poland',
  platform = 'dsk',
  universe = 'adpage'
)

# filter the reference table on the dedicated scope
audit_table <- verticals_reference %>% filter(
  business == audit_scope[['business']]
  & area == audit_scope[['area']]
  & country == audit_scope[['country']]
  & platform == audit_scope[['platform']]
  & universe == audit_scope[['universe']]
  )

audit_type_vector <- unique(audit_table[['audit_type']])












# create function
get_audit_verticals_jql <- function(business,area,country,platform,universe,from,to){
  # create the main() part of JQL
  main_jql <- paste0(
    "function main() {
    return Events({from_date: '",from,"',to_date:   '",to,"'})")
  
  # create the platform filters from JQL
  if (platform == 'dsk'){
    url_cat_jql <- 
    platform_jql <- paste0()
  }
  
  if (business == 'verticals'){
    if (area == 'cars') {
      
    } else if (area == 'realestate'){
      
    } # enf if else if area
  } # end if business verticals
}

for (audit in audit_type_vector){
  
}

verticals_list$cars$poland$db$categories$l1_cat$code

unique(verticals_list[['cars']][['poland']][['db']][['categories']][['l1_cat']][['code']])

default_events_jql <- audit_table %>% filter(audit_type == 'default') %>% select(event)
default_events_jql <- paste0("['",paste(unique(default_events_jql[['event']]),collapse = "','"),"']")
