# Libraries -------------------------------------------------------------------
library("ggplot2")
library("scales")
library("showtext")

# Bullet Graph function -------------------------------------------------------

bullet.graph <- function(bg.data, bg.title, bg.subtitle, maxgb){
  
  # compute max and half for the ticks and labels
  max.bg <- 20000
  mid.bg <- 10000
  
  gg <- ggplot(bg.data) 
  gg <- gg + geom_bar(aes(measure, high),  fill="gray90", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, mean),  fill="blue", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, target),   fill="gray90", stat="identity", width=0.5)
  gg <- gg + geom_text(data=bg.data[nrow(bg.data),],  aes(measure, maxgb, label="target"), color = "#0570b0", vjust = 3, hjust=1,  family="Open Sans")
  gg <- gg + geom_text(data=bg.data[nrow(bg.data),], aes(measure, maxgb, label="actual"), color = "#74a9cf", vjust = -2, hjust=1,  family="Open Sans")
  gg <- gg + geom_text(aes(measure, target, label=paste0("", target)), color = "#0570b0", vjust = 3, hjust=0.5,  family="Open Sans")
  gg <- gg + geom_text(aes(measure, value, label=paste0("",value, "(", var, ")")), color = "#74a9cf", vjust = -2, hjust=0, family="Open Sans", fontface = "bold")
  gg <- gg + geom_bar(aes(measure, value), fill="#74a9cf",  stat="identity", width=0.2) 
  gg <- gg + geom_errorbar(aes(y=target, x=measure, ymin=target, ymax=target), color="#0570b0", width=0.45) 
  gg <- gg + geom_point(aes(measure, target), colour="#0570b0", size=2.5) 
  gg <- gg + scale_y_continuous(breaks=seq(0,maxgb,mid.bg))
  gg <- gg + coord_flip()
  gg <- gg + theme(axis.text.x=element_text(size=8),
                   axis.title.x=element_blank(),
                   axis.line.y=element_blank(), 
                   axis.text.y=element_text(hjust=1, color="black"), 
                   axis.ticks.y=element_blank(),
                   axis.title.y=element_blank(),
                   legend.position="none",
                   panel.background=element_blank(), 
                   panel.border=element_blank(),
                   panel.grid.major=element_blank(),
                   panel.grid.minor=element_blank(),
                   plot.background=element_blank())
  gg <- gg + ggtitle(bg.title, subtitle = bg.subtitle)
  gg <- gg + theme(text=element_text(family = "Open Sans"))+theme(axis.text=element_text(size=12))
  
  return(gg)
}

# Graph For Revenue -----------------------------------------------------------

# df <- data.frame(
#   measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "Indonesia", "Philippines", "TOTAL"),
#   high=c(20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000),
#   mean=c(5000,5000, 5000, 5000, 5000,5000,5000,5000),
#   low=c(2500, 2500, 2500, 2500, 2500, 2500, 2500, 2500), 
# target=c(28154298, 3985518, 5396276, 1461750, 2690000, 1500045, 1000667, 44188554),
# fc1=c(33490561, 5047731, 6418353, 1849202, 2690000, 2439479,  591583, 52526909),
# value=c(36848915, 5346170, 6389074, 2527632, 2338000, 2359173,  465571, 56274535)
# )


 df <- data.frame(
   measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "TOTAL"),
   high=c(20000, 20000, 20000, 0),
   mean=c(5000,5000, 5000, 0),
   low=c(2500, 2500, 2500, 0),
 target=c(28154298, 3985518, 5396276, 37536092),
 fc1=c(33490561, 5047731, 6418353, 44956645),
 value=c(36848915, 5346170, 6389074, 48584159)
)

df$var <- percent(round(df$value/df$target-1,3))
df$var_fc1 <- percent(round(df$value/df$fc1-1,3))
df$target <- round(df$target / 1000000,2)
df$fc1 <- round(df$fc1 / 1000000,2)
df$value <- round(df$value / 1000000, 2)
df$high <- 70


gh_cars_revenue <- bullet.graph(df, "CARS - Revenue", "FYTD Jan/FY18 - M USD", 70)

# Graph for Paying Professional Users ----------------------------------------- 

# df <- data.frame(
#   measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "TOTAL"),
#   high=c(25000, 25000, 25000, 25000, 25000, 25000),
#   mean=c(5000,5000, 5000, 5000, 5000,0),
#   low=c(2500, 2500, 2500, 2500, 2500,0), 
#   target = c(9390, 1050, 3157, 2281, 1098,16976),
#   fc1 = c(9390, 1050, 2989, 3669, 1098,18196),
#   value=c(9910, 1043, 2826, 4592, 269,18640)
# )

df <- data.frame(
  measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT","TOTAL"),
  high=c(25000, 25000, 25000,  25000),
  mean=c(5000,5000, 5000, 0),
  low=c(2500, 2500, 2500, 0), 
  target = c(9390, 1050, 3157, 13597),
  fc1 = c(9390, 1050, 2989, 13429),
  value=c(9910, 1043, 2826, 13779)
)

myNumCols <- which(unlist(lapply(df, is.numeric)))
df[(nrow(df) + 1), myNumCols] <- colSums(df[, myNumCols], na.rm=TRUE)

df$var <- percent(round(df$value/df$target-1,3))

gh_cars_ppu <- bullet.graph(df, "CARS - Paying Professional Users", "FYTD Jan/FY18 - M USD", 25000)