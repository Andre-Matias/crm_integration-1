library(dplyr)
library(tidyr)
library(reshape2)
library(ggplot2)
library(data.table)


###################################################################################################
# OKR1 & OKR2 functions --------------------------------------------------------------------------#
###################################################################################################


# prepare_for_retention(): prepare the 1st OKR dataset for retention analysis ---------------------
## Cleaning
## Convert it into long format
## Summarize by week
## Calculate % retention for each weekly cohort 

prepare_for_retention <- function (country_df) {
  
  clean <- country_df %>%
    select(dates:retainCount.15, platform) %>%
    select(-retainCount.0)
  row.names(clean) <- NULL
  my_interval <- as.character(seq(1,15))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  
  ## From wide to long
  ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
                         as.character(my_interval), factor_key = T)
  
  # Date format and sort
  ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
  ret_any_long <- ret_any_long %>% 
    arrange(platform, Date, TimeToConvert)
  ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
  
  # Calculate week starting on Mondays                      
  ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
  
  #Group by
  ret_any_long <- ret_any_long %>% 
    group_by(platform, week, TimeToConvert) %>% 
    summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
  
  # CTR
  ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
  
  
  # Prepare dataframe for retention
  ret_any_3 <- ret_any_long %>%
    group_by(platform, week, TimeToConvert) %>%
    summarize(ret=mean(CTR, na.rm=T))
  
  return(ret_any_3)
  
}


# to_wide_table(): convert the retention dataframe above into a wide format, easy to visualize a data table -----------
to_wide_table <- function (df_prepared_retention) {
    ret_any_table <- df_prepared_retention %>%
                        mutate(ret_per = round(ret*100, 2))
    ret_any_table <- dcast(ret_any_table, platform + week ~ TimeToConvert, value.var="ret_per")
    return(ret_any_table)
}


# retPlot(): make a retention plot for 1st okr ---------------------------------------------
retPlot <- function (df_prepared_retention, title="", subtitle=""){
  max_y <- max(df_prepared_retention$ret, na.rm = T)+0.02
  ggplot(data = df_prepared_retention) + 
  geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,max_y,0.01), limits = c(0,max_y))+
  scale_x_continuous(breaks = seq(0,15,1), limits = c(0,15)) +
  ggtitle(title, subtitle)+
  theme_fivethirtyeight()+
  theme(text = element_text(family = "Andale Mono")) + 
    xlab("days to convert") + ylab("% new users") 
}


#' daily_okr1(): ---------------------------------------------------------------------------
#' Calculate OKR1 7 days retention on a daily basis
# to be used for comparing original vs variation
daily_okr1 <- function (country_df) {
  daily <- country_df %>%
    select(dates:retainCount.15, platform) 
  row.names(daily) <- NULL
  daily$retainCount.0 <- NULL
  daily$ret_users <- rowSums(daily[, 3:9], na.rm = T)
  daily <- daily %>%
    select (platform, dates, cohortCount, ret_users) %>%
    mutate (ret_per= ret_users / cohortCount) %>%
    mutate( dates = as.Date(as.character(dates)), week = cut(dates, breaks="week", start.on.monday=T))
  return(daily)
}

# retCompPlot(): plot to compare OKR1 original vs variation --------------------------------------
retCompPlot <- function (df_comp, title="", subtitle=""){
  max_y <- max(df_comp$ret_per.x, na.rm = T)+0.02
  ggplot(data = df_comp, aes(dates)) + 
  geom_line(aes(y = ret_per.x, colour = "original (home)"))+ 
  geom_line(aes(y = ret_per.y, colour = "variation (pwa)"))+
  theme(legend.position="bottom")+
  #scale_y_continuous(labels = scales::percent)+
  scale_y_continuous(labels = scales::percent, breaks = seq(0, max_y, 0.02), limits = c(0, max_y))+
  ggtitle(title, subtitle)+
  theme_fivethirtyeight()+
  xlab(label="day of first interaction") + ylab(label="% retention")
#return(p)
}


# test function
# test <- filter(comp_okr2_pl, platform=="rwd")
# ggplot(data = test, aes(dates)) + 
#   geom_line(aes(y = ret_per.x, colour="original (home)"))+ 
#   geom_line(aes(y = ret_per.y, colour="variation (pwa)")) +
#   xlab(label="day") + ylab(label="14 days retention") +
#   theme(legend.position="bottom")


###################################################################################################
# OKR2 functions (for old JSON)                                                                   #
# currently not using it--------------------------------------------------------------------------#
# #################################################################################################

#' cleaning_json(): 
#' clean and put in tabular format lead dataset coming in json format
#' not using it anymore!
cleaning_json <- function(a) { 
  b <- do.call(rbind, a) 
  c <- as.data.frame(t(b))
  d <- as.data.frame(lapply(c, function(x) unlist(x)))
  d$V2 <- row.names(d)
  d$V3 <- str_extract(d$V2,"(\\d{4}-\\d{2}-\\d{1,2})")
  d$V4 <- str_extract(d$V2,"\\d{4}-\\d{2}-\\d{1,2}\\.\\d{1,2}")
  d$V5 <- lapply(d$V4, function(x) strsplit(x, "\\.")[[1]][2])
  ###
  d$desktop <- grepl(pattern = "desktop", d$V2)
  d$rwd <- grepl(pattern = "rwd", d$V2)
  d$android <- grepl(pattern = "android", d$V2)
  d$ios <- grepl(pattern = "ios", d$V2)
  d$platform[d$desktop==TRUE] <- "desktop"
  d$platform[d$rwd==TRUE] <- "rwd"
  d$platform[d$android==TRUE] <- "android"
  d$platform[d$ios==TRUE] <- "ios"
  d <- d %>% select (-c(desktop, rwd, android, ios))
  ####
  d$NewUsers <- grepl(pattern = "\\$New Users", d$V2)
  d$ReturningUsers <- grepl(pattern = "\\$Returning Users", d$V2)
  d$V7 <- grepl(pattern = "\\sNew Users", d$V2)
  d$V8 <- grepl(pattern = "\\sReturning Users", d$V2)
  d$V4 <- NULL
  d$NewUsers[d$NewUsers==TRUE] <- "New Users"
  d$NewUsers[d$ReturningUsers==TRUE] <- "Returning Users"
  d$NewUsers[d$V7==TRUE] <- "New Users"
  d$NewUsers[d$V8==TRUE] <- "Returning Users"
  d$ReturningUsers <- NULL
  d$V7 <- NULL
  d$V8 <- NULL
  d$V9 <- 0
  d$V9[is.na(d$V5)] <- d$V1[is.na(d$V5)]
  d$V9[!is.na(d$V5)] <- NA
  d$V5 <- as.numeric(unlist(d$V5))
  d$V3 <- as.Date(d$V3) 
  # d <- d[d$V3 >= '2017-06-01' & d$V3 < '2017-07-01' , ]
  # Filter only new users acquired between Mon 3 Jul - Sun 6 Ago
  # All have 30 days to convert since data was extracted until 7 Sep
  # or put the start/end dates you like
  # d <- d[d$V3 >= '2017-07-03' & d$V3 <= '2017-12-03' , ]
  
  d[is.na(d$V5), c("V5")] <- -1
  d <- d %>% arrange( platform, V3, NewUsers, V5)
  d$V10 <- na.locf(d$V9) 
  d <- d[d$V5!=-1, ]
  d <- d[, c("platform", "V3", "NewUsers", "V5", "V10", "V1")]
  colnames(d)<-c("platform", "Date", "TypeOfUser", "TimeToConvert", "TotalUsers", "ConvertedUsers")
  d <- d %>% 
    arrange(platform, Date, TypeOfUser, TimeToConvert)
  #keep only new users and max 15 days
  d <- d %>% 
        filter(TypeOfUser=="New Users" & TimeToConvert <=15) %>%
        select(-TypeOfUser)
  #----------
  return(d)
  
}


# prepare_for_retention_lead(): prepare the OKR2 dataset for retention analysis ----------

prepare_for_retention_lead <- function(country_df) {

  # Calculate week starting on Mondays                      
ret_any_long <- mutate (country_df, week = cut(Date, breaks="week", start.on.monday=T))

#Group by
ret_any_long <- ret_any_long %>% 
  group_by(platform, week,TimeToConvert) %>% 
  summarize(TotalUsers=sum(TotalUsers), ConvertedUsers=sum(ConvertedUsers)) 

# CTR
ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers

# Prepare dataframe for retention
ret_any_3 <- ret_any_long %>%
  group_by(platform, week, TimeToConvert) %>%
  summarize(ret=mean(CTR))

return(ret_any_3)
}


# prepare_for_retention_lead_mx(): in case we manually download data from retention tables in Mixpanel
## input a .csv file with retention table
## return df for retention
## first need to change names of columns
prepare_for_retention_lead_mx <- function (country_df) {
  
  clean <- country_df %>%
    select(dates:retainCount.15, platform) %>%
    #select(-retainCount.0)
  row.names(clean) <- NULL
  my_interval <- as.character(seq(0,15))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  
  ## From wide to long
  ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
                         as.character(my_interval), factor_key = T)
  
  # Date format and sort
  ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
  ret_any_long <- ret_any_long %>% 
    arrange(platform, Date, TimeToConvert)
  ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
  
  # Calculate week starting on Mondays                      
  ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
  
  #Group by
  ret_any_long <- ret_any_long %>% 
    group_by(platform, week, TimeToConvert) %>% 
    summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
  
  # CTR
  ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
  
  
  # Prepare dataframe for retention
  ret_any_3 <- ret_any_long %>%
    group_by(platform, week, TimeToConvert) %>%
    summarize(ret=mean(CTR, na.rm=T))
  
  return(ret_any_3)
  
}


prepare_for_retention2 <- function (country_df) {
  
  clean <- country_df %>%
    select(dates:retainCount.15, platform) 
  row.names(clean) <- NULL
  my_interval <- as.character(seq(0,15))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  
  ## From wide to long
  ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
                         as.character(my_interval), factor_key = T)
  
  # Date format and sort
  ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
  ret_any_long <- ret_any_long %>% 
    arrange(platform, Date, TimeToConvert)
  ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
  
  # Calculate week starting on Mondays                      
  ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
  
  #Group by
  ret_any_long <- ret_any_long %>% 
    group_by(platform, week, TimeToConvert) %>% 
    summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
  
  # CTR
  ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
  
  
  # Prepare dataframe for retention
  ret_any_3 <- ret_any_long %>%
    group_by(platform, week, TimeToConvert) %>%
    summarize(ret=mean(CTR, na.rm=T))
  
  return(ret_any_3)
  
}


#' daily_okr2(): ---------------------------------------------------------------------------
#' Calculate OKR2 14 days retention on a daily basis
# to be used for comparing original vs variation
daily_okr2 <- function (country_df) {
  daily <- country_df %>%
    select(dates:retainCount.15, platform) 
  row.names(daily) <- NULL
  #daily$retainCount.0 <- NULL
  daily$ret_users <- rowSums(daily[, 3:16], na.rm = T)
  daily <- daily %>%
    select (platform, dates, cohortCount, ret_users) %>%
    mutate (ret_per= ret_users / cohortCount) %>%
    mutate( dates = as.Date(as.character(dates)), week = cut(dates, breaks="week", start.on.monday=T))
  return(daily)
}





# --------------------------------------------------------------------
# functions to test -----------------
# prepare_for_retention <- function (country_df, same_day="no") {
#   
#   clean <- country_df %>%
#     select(dates:retainCount.15, platform) 
#   # %>%
#   #           if_else (same_day=="no", select(-retainCount.0), select()) 
#   row.names(clean) <- NULL
#   if(same_day=="no") {clean$retainCount.0 <- NULL}
#   
#   #if (same_day=="no") {my_interval <- as.character(seq(1,15))} else {my_interval <- as.character(seq(0,15))}
#   #my_interval <- ifelse(same_day=="no", as.character(seq(1,15)), as.character(seq(0,15)) )
#   #return (clean)}
#   my_interval <- as.character(seq(1,15))
#   names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
#   
#   ## From wide to long
#   ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
#                          as.character(my_interval), factor_key = T)
#   
#   # Date format and sort
#   ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
#   ret_any_long <- ret_any_long %>% 
#     arrange(platform, Date, TimeToConvert)
#   ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
#   
#   # Calculate week starting on Mondays                      
#   ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
#   
#   #Group by
#   ret_any_long <- ret_any_long %>% 
#     group_by(platform, week, TimeToConvert) %>% 
#     summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
#   
#   # CTR
#   ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
#   
#   
#   # Prepare dataframe for retention
#   ret_any_3 <- ret_any_long %>%
#     group_by(platform, week, TimeToConvert) %>%
#     summarize(ret=mean(CTR, na.rm=T))
#   
#   return(ret_any_3)
#   
# }
