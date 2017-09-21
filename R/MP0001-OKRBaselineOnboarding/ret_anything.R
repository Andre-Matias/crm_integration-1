#' Retention code consolidating data from PL, PT, RO
#' 

library(tidyr)

#setwd("~/verticals-bi/R/MP0001-OKRBaselineOnboarding")

ret_any_pt <- read.csv("data/retention_any_pt.csv", sep=",")


# Prepare the dataset
ret_any_pl <- read.csv("data/retention_any_2017-07-03_to_2017-09-17.csv", sep=";")
ret_any <- ret_any_pl  # then use this for all countries
sixty <- as.character(seq(0,60))
names(ret_any) <- c("Date", "TotalUsers", sixty)
ret_any <- ret_any[, 1:33]

# From wide to long
#ret_any_long <- gather(ret_any, TimeToConvert, Converted, X0.days.later:X30.days.later, factor_key = T)
ret_any_long <- gather(ret_any, TimeToConvert, ConvertedUsers, as.character(0:30), factor_key = T)

ret_any_long$Date <- as.Date(as.character(ret_any_long$Date),"%d/%m/%Y")
ret_any_long <- ret_any_long %>% 
                  arrange(Date, TimeToConvert)

# removing 0 time to convert as doesnt make sense (several events are fired on first visit)
ret_any_long$TimeToConvert <- as.numeric(as.character(ret_any_long$TimeToConvert))
ret_any_long <- filter (ret_any_long, TimeToConvert > 0)


# CTR
ret_any_long$CTR <- ret_any_long$ConvertedUsers / ret_any_long$TotalUsers

# # group by
# ret_any_d <- ret_any_long %>% 
#               group_by(Date) %>% 
#               mutate(RollingSum = cumsum(CTR))

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
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Return and do Anything - Time to Return (days)", subtitle = "otomoto.pl")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert") + ylab("% new users") 

ret_any_plot 


# Put data into a wide table also (convert long to wide format)
ret_any_table <- ret_any_2 %>%
  mutate(ret_per = round(ret*100, 1))
ret_any_table <- dcast(ret_any_table, week ~ TimeToConvert, value.var="ret_per")
