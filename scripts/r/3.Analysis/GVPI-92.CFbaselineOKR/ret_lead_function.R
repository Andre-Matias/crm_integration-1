#' Retention code each country separate
#' all in a function as per point 3)
#' since I have to replicate it for PL, PT, RO
#' 
#' 1) downnload each JQL result in json format
#' 2) import it into R and assign it to a object
#' 3) apply prepare_for_retention() function containing retention code. Output a table with retention by week.
#' 4) Build ggplot on each country output
#' 5) Combine plots into a dashboard if necessary

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

# Define function
prepare_for_retention <- function(a) { 
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
  d <- d[d$V3 >= '2017-07-03' & d$V3 < '2017-08-06' , ]
  
  d[is.na(d$V5), c("V5")] <- -1
  d <- d %>% arrange(V3, NewUsers, V5)
  d$V10 <- na.locf(d$V9) 
  d <- d[d$V5!=-1, ]
  d <- d[, c("V3", "NewUsers", "V5", "V10", "V1")]
  colnames(d)<-c("Date", "TypeOfUser", "TimeToConvert", "TotalUsers", "ConvertedUsers")
  d <- d %>% 
    arrange(Date, TypeOfUser, TimeToConvert)
  
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
  
  # Prepare dataframe for retention
  dNewUsers2 <- dNewUsers %>%
    group_by(week,TimeToConvert) %>%
    summarize(ret=mean(RollingSum))
  
  return(dNewUsers2)
                  
}



# Otomoto PL dataset ---------------------------------------
a <- fromJSON(file = "retention_lead_pl_3Jul_7Sep.json")
ret_df_pl <- prepare_for_retention(a)

## ggplot
ret_plot_pl <- 
  ggplot(data=ret_df_pl)+geom_line(aes(x=TimeToConvert, y=ret, colour=week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,0.25,0.01), limits = c(0,0.25))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "otomoto.pl")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert")+ylab("% new users") 

ret_plot_pl


# Standvirtual PT ---------------------------------------
a <- fromJSON(file = "retention_lead_pt_3Jul_7Sep.json")
ret_df_pt <- prepare_for_retention(a)

## ggplot
ret_plot_pt <- 
  ggplot(data=ret_df_pt)+geom_line(aes(x=TimeToConvert, y=ret, colour=week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,0.25,0.01), limits = c(0,0.25))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "standvirtual.pt")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert")+ylab("% new users") 

ret_plot_pt


# Autovit RO ---------------------------------------
a <- fromJSON(file = "retention_lead_ro_3Jul_7Sep.json")
ret_df_ro <- prepare_for_retention(a)

## ggplot
ret_plot_ro <- 
  ggplot(data=ret_df_ro)+geom_line(aes(x=TimeToConvert, y=ret, colour=week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,0.25,0.01), limits = c(0,0.25))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "autovit.ro")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert")+ylab("% new users") 

ret_plot_ro


#'------------------------------------------------
#'Combine plots

# gb1 <- ggplot_build(ret_plot_pl)
# gb2 <- ggplot_build(ret_plot_pt)
# 
# n1 <- length(gb1$layout$panel_params[[1]]$y.labels)
# n2 <- length(gb2$layout$panel_params[[1]]$y.labels)
# 
# gA <- ggplot_gtable(gb1)
# gB <- ggplot_gtable(gb2)
# plot(rbind(gA, gB))
# plot(rbind(ret_plot_pl, ret_plot_pt, ret_plot_ro))

grid.arrange(ret_plot_pl, ret_plot_pt, ret_plot_ro, nrow=2, ncol=2) 


