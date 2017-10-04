#' Retention code consolidating data from PL, PT, RO
#' use a function as per point 3)
#' since I have to replicate processing for PL, PT, RO
#' 
#' 1) downnload each JQL result in json format
#' 2) import it into R and assign it to a object
#' 3) apply prepare_for_consolidation() function

#' 
#' Consolidate dataframes from different countries
#' Complete calculations prepare a consolidate dataframe ready to be plot


# Set working directory
# setwd("~/verticals-bi/scripts/r/3.Analysis/GVPI-92.CFbaselineOKR")

# Load libraries
library("rjson")
library("data.table")
library("dplyr")
library("dtplyr")
library("stringr")
library("zoo")
library("ggplot2")
library("ggthemes")
library('scales')
library("gridExtra")

# Define prepare_for_consolidation() function
prepare_for_consolidation <- function(a) { 
  b <- do.call(rbind, a) 
  c <- as.data.frame(t(b))
  d <- as.data.frame(lapply(c, function(x) unlist(x)))
  d$V2 <- row.names(d)
  d$V3 <- str_extract(d$V2,"(\\d{4}-\\d{2}-\\d{1,2})")
  d$V4 <- str_extract(d$V2,"\\d{4}-\\d{2}-\\d{1,2}\\.\\d{1,2}")
  d$V5 <- lapply(d$V4, function(x) strsplit(x, "\\.")[[1]][2])
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
  d <- d[d$V3 >= '2017-07-03' & d$V3 <= '2017-09-24' , ]
  
  d[is.na(d$V5), c("V5")] <- -1
  d <- d %>% arrange(V3, NewUsers, V5)
  d$V10 <- na.locf(d$V9) 
  d <- d[d$V5!=-1, ]
  d <- d[, c("V3", "NewUsers", "V5", "V10", "V1")]
  colnames(d)<-c("Date", "TypeOfUser", "TimeToConvert", "TotalUsers", "ConvertedUsers")
  d <- d %>% 
    arrange(Date, TypeOfUser, TimeToConvert)
  
  #----------
  return(d)
  
}

# Read in json files
  json_pl <- fromJSON(file = "data/retention_lead_pl.json")
  json_pt <- fromJSON(file = "data/retention_lead_pt.json")
  json_ro <- fromJSON(file = "data/retention_lead_ro.json")

# Prepare for consolidation  
pl <- prepare_for_consolidation(json_pl)
pt <- prepare_for_consolidation(json_pt)
ro <- prepare_for_consolidation(json_ro)

# Bind dataframes
bind <- rbind(pl, pt, ro)

# Group by date, typeofuser, time to convert
d <- bind %>%
        group_by(Date, TypeOfUser, TimeToConvert) %>%
        summarise(TotalUsers= sum(TotalUsers), ConvertedUsers= sum(ConvertedUsers))


# Go on with code
d$CTR <- d$ConvertedUsers / d$TotalUsers
  
  d <- d %>% 
    group_by(Date, TypeOfUser) %>% 
    mutate(RollingSum = cumsum(CTR), TotalConvertedByDay = sum(ConvertedUsers), 
           PerConvertedUsers = ConvertedUsers/TotalConvertedByDay, 
           RollingPerConvertedUsers = cumsum(PerConvertedUsers) )
  
  dNewUsers <- d %>% 
    filter(TypeOfUser=="New Users") %>% 
    filter(TimeToConvert < 31)  # filter users converting within 30 days
  
  # Calculate week starting on Mondays                      
  dNewUsers <- mutate (dNewUsers, week = cut(Date,breaks="week", start.on.monday=T))
  
  # Calculate number of new users weekly
  newWeekly <- dNewUsers %>%
                 group_by(week,Date) %>%
                 summarize(new_users_day= mean(TotalUsers)) %>%
                 group_by(week) %>%
                 summarize(new_users_week= sum(new_users_day))
  
  # Prepare dataframe for retention
  dNewUsers2 <- dNewUsers %>%
    group_by(week,TimeToConvert) %>%
    summarize(ret=mean(RollingSum))
  
 

# Visualization (all countries aggregated)
  
## ggplot
ret_plot_cons <- 
  ggplot(data = dNewUsers2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,0.20,0.01), limits = c(0,0.20))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is cumulated")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 

ret_plot_cons 




# Put data into a wide table also (convert long to wide format)
ret_table_cons <- dNewUsers2 %>%
                   mutate(ret_per = round(ret*100, 1))
ret_table_cons <- dcast(ret_table_cons, week ~ TimeToConvert, value.var="ret_per")


