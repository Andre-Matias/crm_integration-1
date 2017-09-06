library("rjson")
library("data.table")
library("dplyr")
library("dtplyr")
library("stringr")
library("zoo")
library("ggplot2")
library("ggthemes")

options(scipen=999)

a <- fromJSON(file = "Retention_Marco_3-7to5-9.json.json")
a <- fromJSON(file = "retention_otomoto_pl.json")
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
d <- d[d$V3 >= '2017-06-01' & d$V3 < '2017-07-01' , ]
d[is.na(d$V5), c("V5")] <- -1
d <- d %>% arrange(V3, NewUsers, V5)
d$V10 <- na.locf(d$V9) 
d <- d[d$V5!=-1, ]
d <- d[, c("V3", "NewUsers", "V5", "V10", "V1")]
colnames(d)<-c("Date", "TypeOfUser", "TimeToConvert", "TotalUsers", "ConvertedUsers")
d <- d %>% arrange(Date, TypeOfUser, TimeToConvert)

d$CTR <- d$ConvertedUsers / d$TotalUsers

d <- d %>% group_by(Date, TypeOfUser) %>% mutate(RollingSum = cumsum(CTR), TotalConvertedByDay = sum(ConvertedUsers), PerConvertedUsers = ConvertedUsers/TotalConvertedByDay, RollingPerConvertedUsers = cumsum(PerConvertedUsers) )

dNewUsers <- d %>% filter(TypeOfUser=="New Users")


ggplot(data=dNewUsers)+geom_smooth(aes(x=TimeToConvert, y=RollingPerConvertedUsers, colour=Date))+
  scale_y_continuous(labels = percent, breaks = seq(0,1,0.1), limits = c(0,1))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "otomoto.pl")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert")+ylab("% buyers")


ggplot(data=dNewUsers)+geom_smooth(aes(x=TimeToConvert, y=RollingSum, colour=Date))+
  scale_y_continuous(labels = percent, breaks = seq(0,0.2,0.01), limits = c(0,0.2))+
  scale_x_continuous(breaks = seq(0,30,1), limits = c(0,30))+ggtitle("New Users That Send a Lead - Time to Send (days)", subtitle = "otomoto.pl")+
  theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+xlab("days to convert")+ylab("% buyers")

steps <- factor(c("home", "listing", "ad detail page", "lead"), levels=c("home", "listing", "ad detail page", "lead"), ordered=TRUE)

df <-
  data.frame( steps = steps,
              values = c(3326752/3326752, 2871078/3326752, 2612983/3326752, 330146/3326752)
  )

ggplot(data=df)+geom_bar(stat="identity",aes(steps, values))+theme_fivethirtyeight()+theme(text = element_text(family = "Andale Mono"))+
  scale_y_continuous(labels = percent)+geom_text(aes(steps, values, label=percent(round(values,3))), family = "Andale Mono", vjust=-0.5)+
  ggtitle("Buyer's Browsing Funnel", subtitle = "otomoto.pl - 30 days convertion window")




