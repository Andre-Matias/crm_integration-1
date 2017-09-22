#' Retention code consolidating data from PL, PT, RO
#' Extract data every Monday from Mixpanel retention report


library(tidyr)

#setwd("~/verticals-bi/R/MP0001-OKRBaselineOnboarding")

# Read and bind datasets for PL, PT, RO
ret_any_pl <- read.csv("data/retention_any_pl.csv")
ret_any_pt <- read.csv("data/retention_any_pt.csv")
ret_any_ro <- read.csv("data/retention_any_ro.csv")
ret_any <- rbind(ret_any_pl, ret_any_pt,ret_any_ro)

sixty <- as.character(seq(0,60))
names(ret_any) <- c("Date", "TotalUsers", sixty)
ret_any <- ret_any[, 1:33]

# From wide to long
#ret_any_long <- gather(ret_any, TimeToConvert, Converted, X0.days.later:X30.days.later, factor_key = T)
ret_any_long <- gather(ret_any, TimeToConvert, ConvertedUsers, as.character(0:30), factor_key = T)

# Group by date, typeofuser, time to convert since I need to consolidate countries
ret_any_long <- ret_any_long %>%
  group_by(Date, TimeToConvert) %>%
  summarise(TotalUsers= sum(TotalUsers), ConvertedUsers= sum(ConvertedUsers))

# Date format and sort
ret_any_long$Date <- as.Date(as.character(ret_any_long$Date))
ret_any_long <- ret_any_long %>% 
  arrange(Date, TimeToConvert)

# Removing 0 time to convert as doesnt make sense (several events are fired on first visit)
ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
ret_any_long <- filter (ret_any_long, TimeToConvert > 0)

# CTR
ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers

# Calculate week starting on Mondays                      
ret_any_long <- mutate (ret_any_long, week = cut(Date,breaks="week", start.on.monday=T))

# Prepare dataframe for retention
ret_any_2 <- ret_any_long %>%
  group_by(week,TimeToConvert) %>%
  summarize(ret=mean(CTR))

## ggplot
ret_any_plot <- 
  ggplot(data = ret_any_2) + geom_line(aes(x = TimeToConvert, y = ret, colour = week))+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,0.13,0.01), limits = c(0,0.13))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Return and do Anything - Time to Return (days)", subtitle = "Consolidated: otomoto.pl + standvirtual.pt + autovit.ro. Retention % is not cumulated")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 

ret_any_plot 

# Put data into a wide table also (convert long to wide format)
ret_any_table <- ret_any_2 %>%
  mutate(ret_per = round(ret*100, 1))
ret_any_table <- dcast(ret_any_table, week ~ TimeToConvert, value.var="ret_per")
