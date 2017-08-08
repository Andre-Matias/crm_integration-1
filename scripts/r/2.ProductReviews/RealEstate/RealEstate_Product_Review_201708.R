# Libraries -------------------------------------------------------------------
library("ggplot2")
library("scales")

# Revenue ---------------------------------------------------------------------

# Paying Professional Users ---------------------------------------------------

# Bullet Graph function -------------------------------------------------------

bullet.graph <- function(bg.data, bg.title, bg.subtitle, maxgb){
  
  # compute max and half for the ticks and labels
  max.bg <- 20000
  mid.bg <- 10000
  
  gg <- ggplot(bg.data) 
  gg <- gg + geom_bar(aes(measure, high),  fill="gray90", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, mean),  fill="blue", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, target),   fill="gray90", stat="identity", width=0.5)
  gg <- gg + geom_text(data=bg.data[6,],  aes(measure, maxgb, label="target"), color = "salmon1", vjust = 3, hjust=1,  family="Andale Mono")
  gg <- gg + geom_text(data=bg.data[6,], aes(measure, maxgb, label="actual"), color = "salmon4", vjust = -2, hjust=1,  family="Andale Mono")
  gg <- gg + geom_text(aes(measure, target, label=paste0("", target)), color = "salmon1", vjust = 3, hjust=0.5,  family="Andale Mono")
  gg <- gg + geom_text(aes(measure, value, label=paste0("",value, "(", var, ")")), color = "salmon4", vjust = -2, hjust=0, family="Andale Mono", fontface = "bold")
  gg <- gg + geom_bar(aes(measure, value), fill="salmon4",  stat="identity", width=0.2) 
  gg <- gg + geom_errorbar(aes(y=target, x=measure, ymin=target, ymax=target), color="salmon1", width=0.45) 
  gg <- gg + geom_point(aes(measure, target), colour="salmon1", size=2.5) 
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
  gg <- gg + theme(text=element_text(family = "Andale Mono"))+theme(axis.text=element_text(size=12))
  
  return(gg)
  
}

# Graph For Revenue -----------------------------------------------------------

df <- data.frame(
  measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "TOTAL"),
  high=c(20000, 20000, 20000, 20000, 20000, 20000),
  mean=c(5000,5000, 5000, 5000, 5000,0),
  low=c(2500, 2500, 2500, 2500, 2500,0), 
  target=c(10847964, 1513986, 2066357, 663447, 793000, 15884754),
  value=c(13561585, 1955805, 2361115, 714917, 753000, 19346422)
)


df$var <- percent(round(df$value/df$target-1,3))
df$target <- round(df$target / 1000000,2)
df$value <- round(df$value / 1000000, 2)
df$high <- 30


gh_cars_revenue <- bullet.graph(df, "CARS - Revenue", "FYTD July/FY18 - M USD", 30)

# Graph for Paying Professional Users ----------------------------------------- 

df <- data.frame(
  measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "TOTAL"),
  high=c(25000, 25000, 25000, 25000, 25000, 25000),
  mean=c(5000,5000, 5000, 5000, 5000,0),
  low=c(2500, 2500, 2500, 2500, 2500,0), 
  target=c(9190, 960, 2709, 3022, 733, 16614),
  value=c(9477, 933, 2637, 3097, 339, 16483)
)

#myNumCols <- which(unlist(lapply(df, is.numeric)))
#df[(nrow(df) + 1), myNumCols] <- colSums(df[, myNumCols], na.rm=TRUE)

df$var <- percent(round(df$value/df$target-1,3))

gh_cars_ppu <- bullet.graph(df, "CARS - Paying Professional Users", "FYTD July/FY18 ", 25000)