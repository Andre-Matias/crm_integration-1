
###################################################################################################
# OKR1 & OKR2 functions --------------------------------------------------------------------------#
###################################################################################################

library(dplyr)
library(tidyr)
library(reshape2)
library(ggplot2)
library(data.table)


# prepare_for_retention(): prepare the 1st OKR dataset for retention analysis ---------------------
## Cleaning
## Convert it into long format
## Summarize by week
## Calculate % retention for each weekly cohort 


# new considering version
# prepare_for_retention <- function (country_df) {
#   
#   clean <- country_df %>%
#     select(dates:retainCount.15, platform, version) %>%
#     select(-retainCount.0)
#   row.names(clean) <- NULL
#   my_interval <- as.character(seq(1,15))
#   names(clean) <- c("Date", "TotalUsers", my_interval, "platform", "version")
#   
#   ## From wide to long
#   ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
#                          as.character(my_interval), factor_key = T)
#   
#   # Date format and sort
#   ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
#   ret_any_long <- ret_any_long %>% 
#     arrange(platform, version, Date, TimeToConvert)
#   ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
#   
#   # Calculate week starting on Mondays                      
#   ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
#   
#   #Group by
#   ret_any_long <- ret_any_long %>% 
#     group_by(platform, version, week, TimeToConvert) %>% 
#     summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
#   
#   # CTR
#   ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
#   
#   
#   # Prepare dataframe for retention
#   ret_any_3 <- ret_any_long %>%
#     group_by(platform, version, week, TimeToConvert) %>%
#     summarize(ret=mean(CTR, na.rm=T))
#   
#   return(ret_any_3)
#   
# }
# 
# 
# 
# prepare_for_retention2 <- function (country_df) {
#   
#   clean <- country_df %>%
#     select(dates:retainCount.15, platform, version) 
#   row.names(clean) <- NULL
#   my_interval <- as.character(seq(0,15))
#   names(clean) <- c("Date", "TotalUsers", my_interval, "platform", "version")
#   
#   ## From wide to long
#   ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
#                          as.character(my_interval), factor_key = T)
#   
#   # Date format and sort
#   ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
#   ret_any_long <- ret_any_long %>% 
#     arrange(platform, version, Date, TimeToConvert)
#   ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
#   
#   # Calculate week starting on Mondays                      
#   ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
#   
#   #Group by
#   ret_any_long <- ret_any_long %>% 
#     group_by(platform, version, week, TimeToConvert) %>% 
#     summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
#   
#   # CTR
#   ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
#   
#   
#   # Prepare dataframe for retention
#   ret_any_3 <- ret_any_long %>%
#     group_by(platform, version, week, TimeToConvert) %>%
#     summarize(ret=mean(CTR, na.rm=T))
#   
#   return(ret_any_3)
# }


# Considering version, including same_day argument to be able to use it for both okr1 and okr2 raw data frames.
prepare_for_retention <- function (country_df, same_day="yes") {
  
  clean <- country_df %>%
    select(dates:retainCount.15, platform, version)
  
  my_interval <- as.character(seq(0,15))
  
  if (!(same_day=="yes")) { 
    #clean <- filter (clean, -retainCount.0) 
    clean$retainCount.0 <- NULL  
    my_interval <- as.character(seq(1,15))
  }
  # had issues to use if statement with dplyr code inside !
  
  row.names(clean) <- NULL
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform", "version")
  
  ## From wide to long
  ret_any_long <- gather(clean, TimeToConvert, ConvertedUsers, 
                         as.character(my_interval), factor_key = T)
  
  # Date format and sort
  ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
  ret_any_long <- ret_any_long %>% 
    arrange(platform, version, Date, TimeToConvert)
  ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
  
  # Calculate week starting on Mondays                      
  ret_any_long <- mutate (ret_any_long, week = cut(Date, breaks="week", start.on.monday=T))
  
  #Group by
  ret_any_long <- ret_any_long %>% 
    group_by(platform, version, week, TimeToConvert) %>% 
    summarize(TotalUsers=sum(TotalUsers, na.rm=T), ConvertedUsers=sum(ConvertedUsers, na.rm=T)) 
  
  # CTR
  ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers
  
  
  # Prepare dataframe for retention
  ret_any_3 <- ret_any_long %>%
    group_by(platform, version, week, TimeToConvert) %>%
    summarize(ret=mean(CTR, na.rm=T))
  
  return(ret_any_3)
  
}




#' to_wide_table(): ---------------------------------------------------------------------
#' convert the retention dataframe above into a wide format, easy to visualize a data table -----------
to_wide_table <- function (df_prepared_retention) {
    ret_any_table <- df_prepared_retention %>%
                        mutate(ret_per = round(ret*100, 2))
    ret_any_table <- dcast(ret_any_table, platform + week + version ~ TimeToConvert, value.var="ret_per")
    return(ret_any_table)
}



#' retPlot():  ------------------------------------------------------------------------------------
#' make a retention plot for 1st okr
retPlot <- function (df_prepared_retention, title="", subtitle=""){
  max_y <- max(df_prepared_retention$ret, na.rm = T)+0.02
  ggplot(data = df_prepared_retention) + 
  geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0, max_y,0.01), limits = c(0,max_y))+
  scale_x_continuous(breaks = seq(0,15,1), limits = c(0,15)) +
  ggtitle(title, subtitle)+
  theme_fivethirtyeight()+
  # theme(text = element_text(family = "Andale Mono")) + 
    xlab("days to convert") + ylab("% new users") + scale_colour_grey()
}


#' daily_okr1(): ---------------------------------------------------------------------------
#' Calculate OKR1 7 days retention on a daily basis
#  to be used for comparing original vs variation
daily_okr1 <- function (country_df) {
  daily <- country_df %>%
    select(dates:retainCount.15, platform, version) 
  row.names(daily) <- NULL
  daily$retainCount.0 <- NULL
  daily$ret_users <- rowMeans(daily[, 3:9], na.rm = T)
  daily <- daily %>%
    select (platform, version, dates, cohortCount, ret_users) %>%
    mutate (ret_per= ret_users / cohortCount) %>%
    mutate( dates = as.Date(as.character(dates)), week = cut(dates, breaks="week", start.on.monday=T))
  return(daily)
}


#' retCompPlot():  --------------------------------------------------------------------------------
#' plot to compare OKR1 original vs variations
retCompPlot <- function (df_comp, title="", subtitle=""){
  max_y <- max(df_comp$ret_per, na.rm = T) +0.02
  ggplot(data = df_comp, aes(dates)) + 
    geom_line(aes(y = ret_per, colour = version), size=1.5)+ 
    theme(legend.position="bottom")+
    #scale_y_continuous(labels = scales::percent)+
    scale_y_continuous(labels = scales::percent, breaks = seq(0, max_y, 0.025), limits = c(0, max_y))+
    ggtitle(title, subtitle)+
    theme_fivethirtyeight()+
    xlab(label="day of first interaction") + ylab(label="% retention") 
}

# test function
# test <- filter(comp_okr2_pl, platform=="rwd")
# ggplot(data = test, aes(dates)) + 
#   geom_line(aes(y = ret_per.x, colour="original (home)"))+ 
#   geom_line(aes(y = ret_per.y, colour="variation (pwa)")) +
#   xlab(label="day") + ylab(label="14 days retention") +
#   theme(legend.position="bottom")




#' daily_okr2(): ---------------------------------------------------------------------------
#' Calculate OKR2 14 days retention on a daily basis
# to be used for comparing original vs variation
daily_okr2 <- function (country_df) {
  daily <- country_df %>%
    select(dates:retainCount.15, platform, version) 
  row.names(daily) <- NULL
  #daily$retainCount.0 <- NULL
  daily$ret_users <- rowMeans(daily[, 3:16], na.rm = T)
  daily <- daily %>%
    select (platform, version, dates, cohortCount, ret_users) %>%
    mutate (ret_per= ret_users / cohortCount) %>%
    mutate( dates = as.Date(as.character(dates)), week = cut(dates, breaks="week", start.on.monday=T))
  return(daily)
}


#' #' daily_okr1(): ---------------------------------------------------------------------------
#' #' Calculate OKR1 7 days retention on a daily basis
#' # to be used for comparing original vs variation
#' daily_okr1 <- function (country_df) {
#'   daily <- country_df %>%
#'     select(dates:retainCount.15, platform, version) 
#'   row.names(daily) <- NULL
#'   daily$retainCount.0 <- NULL
#'   daily$ret_users <- rowMeans(daily[, 3:9], na.rm = T)
#'   daily <- daily %>%
#'     select (platform, version, dates, cohortCount, ret_users) %>%
#'     mutate (ret_per= ret_users / cohortCount) %>%
#'     mutate( dates = as.Date(as.character(dates)), week = cut(dates, breaks="week", start.on.monday=T))
#'   return(daily)
#' }


