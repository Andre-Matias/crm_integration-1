# TEST new dashboard
# ggplot multiple versions in the same plot

original <- daily_okr1_pl %>%
  mutate (version = "original")
ver_1 <- daily_okr1_pl_pwa %>%
  mutate (version = "ver_1")
ver_2 <- daily_okr1_pl_pwa %>%
  mutate (version = "ver_2")
ver_2$ret_per <- rnorm(224, 0.2)
comp_test <- rbind(original, ver_1, ver_2)
comp_test <- filter (comp_test, platform=="rwd")

ggplot(data = comp_test, mapping=aes(x=dates, y=ret_per)) + 
  geom_line(mapping= aes(colour = version))


# retCompPlot(): plot to compare OKR1 original vs variation --------------------------------------
retCompPlot <- function (df_comp, title="", subtitle=""){
  max_y <- max(df_comp$ret_per, na.rm = T)+0.02
  ggplot(data = df_comp, aes(dates)) + 
    geom_line(aes(y = ret_per, colour = version))+ 
    theme(legend.position="bottom")+
    #scale_y_continuous(labels = scales::percent)+
    scale_y_continuous(labels = scales::percent, breaks = seq(0, max_y, 0.10), limits = c(0, max_y))+
    ggtitle(title, subtitle)+
    theme_fivethirtyeight()+
    xlab(label="day of first interaction") + ylab(label="% retention")
}

retCompPlot(comp_test)

retCompPlot(comp_okr1_pl)

