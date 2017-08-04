# Revenue ---------------------------------------------------------------------

# Paying Professional Users ---------------------------------------------------

# Autovit ---------------------------------------------------------------------
  # 2016-01	683
  # 2016-02	768
  # 2016-03	831
  # 2016-04	782
  # 2016-05	830
  # 2016-06	817
  # 2016-07	871
  # 2016-08	868
  # 2016-09	853
  # 2016-10	865
  # 2016-11	880
  # 2016-12	884
  # 2017-01	911
  # 2017-02	937
  # 2017-03	937
  # 2017-04	940
  # 2017-05	971
  # 2017-06	936
  # 2017-07	933
  # 2017-08	242

# Standvirtual ----------------------------------------------------------------
  # 2017-01-31	2740
  # 2017-02-28	2677
  # 2017-03-31	2698
  # 2017-04-30	2599
  # 2017-05-31	2681
  # 2017-06-30	2632
  # 2017-07-31	2637
  # 2017-08-31	1496

# India -----------------------------------------------------------------------
  # 2017-04	2464
  # 2017-05	2616
  # 2017-06	2987
  # 2017-07	3097

# LATAM -----------------------------------------------------------------------
  # 201701	90
  # 201702	227
  # 201703	334
  # 201704	331
  # 201705	338
  # 201706	312
  # 201707	339
  # 201708	119


# Graph For Revenue -----------------------------------------------------------

# Graph for Paying Professional Users ----------------------------------------- 

bullet.graph <- function(bg.data, bg.title, bg.subtitle, maxgb){
  
  # compute max and half for the ticks and labels
  max.bg <- 20000
  mid.bg <- 10000
  
  gg <- ggplot(bg.data) 
  gg <- gg + geom_bar(aes(measure, high),  fill="gray90", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, mean),  fill="blue", stat="identity", width=0.5, alpha=0.2) 
  #gg <- gg + geom_bar(aes(measure, target),   fill="gray90", stat="identity", width=0.5)
  gg <- gg + geom_text(data=bg.data[6,],  aes(measure, maxgb, label="target"), color = "#0570b0", vjust = 3, hjust=1,  family="Andale Mono")
  gg <- gg + geom_text(data=bg.data[6,], aes(measure, maxgb, label="actual"), color = "#74a9cf", vjust = -2, hjust=1,  family="Andale Mono")
  gg <- gg + geom_text(aes(measure, target, label=paste0("", target)), color = "#0570b0", vjust = 3, hjust=0.5,  family="Andale Mono")
  gg <- gg + geom_text(aes(measure, value, label=paste0("",value, "(", var, ")")), color = "#74a9cf", vjust = -2, hjust=0, family="Andale Mono", fontface = "bold")
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
  gg <- gg + theme(text=element_text(family = "Andale Mono"))+theme(axis.text=element_text(size=12))
  
  return(gg)
  
}


df <- data.frame(
  measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "TOTAL"),
  high=c(25000, 25000, 25000, 25000, 25000, 25000),
  mean=c(5000,5000, 5000, 5000, 5000,0),
  low=c(2500, 2500, 2500, 2500, 2500,0), 
  target=c(9190, 960, 2669, 2921, 653, 16501),
  value=c(9497, 938, 2632, 3033, 639, 16678)
)

#myNumCols <- which(unlist(lapply(df, is.numeric)))
#df[(nrow(df) + 1), myNumCols] <- colSums(df[, myNumCols], na.rm=TRUE)

df$var <- percent(round(df$value/df$target-1,3))

gh_cars_ppu <- bullet.graph(df, "CARS - Paying Professional Users", "FYTD June/FY18 ", 25000)



df <- data.frame(
  measure=c("Otomoto.PL", "Autovit.RO", "Standvirtual.PT", "Stradia.IN", "Stradia LATAM", "TOTAL"),
  high=c(20000, 20000, 20000, 20000, 20000, 20000),
  mean=c(5000,5000, 5000, 5000, 5000,0),
  low=c(2500, 2500, 2500, 2500, 2500,0), 
  target=c(8238426, 1130731, 1552869, 487623, 561000, 11970649),
  value=c(9969380, 1458494, 1788792, 513750, 524000, 14254416)
)

df$var <- percent(round(df$value/df$target-1,3))
df$target <- round(df$target / 1000000,2)
df$value <- round(df$value / 1000000, 2)
df$high <- 20


gh_cars_revenue <- bullet.graph(df, "CARS - Revenue", "FYTD June/FY18 - M USD", 20)






