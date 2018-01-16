#otomotopl

# 9828421225:9830182098 - grey button
# 9828421225:9831732375 - blue button
# 9828421225:9835432620 - no button - 


df <-
  data.frame(
    id = c("9830182098", "9831732375"),
    name = c("Grey Button", "Blue Button"),
    period = c("Dec 17, 2017 - Jan 07, 2018", "Dec 17, 2017 - Jan 07, 2018"),
    PopulationMetric = c("my_ads_active","my_ads_active"),
    PopulationValue = c(22792 , 22418),
    SucessMetric = c("promote_in_olx","promote_in_olx"),
    SucessValue = c(498, 754)
  )

df$ctr <- 
  df$SucessValue / df$PopulationValue

testResults <- 
  power.prop.test(
    n = df$PopulationValue, 
    p1 = df$ctr[1],
    p2 = df$ctr[2],
    sig.level = 0.05,
    alternative = "two.sided"
  )

otomotoData <- df
otomotoTestResults <- testResults

#autovit

df <-
  data.frame(
    id = c("", ""),
    name = c("Grey Button", "Blue Button"),
    period = c("Dec 17, 2017 - Jan 07, 2018", "Dec 17, 2017 - Jan 07, 2018"),
    PopulationMetric = c("my_ads_active","my_ads_active"),
    PopulationValue = c( 2275, 2134),
    SucessMetric = c("promote_in_olx","promote_in_olx"),
    SucessValue = c(53, 57)
  )

df$ctr <- 
  df$SucessValue / df$PopulationValue

testResults <- 
  power.prop.test(
    n = df$PopulationValue, 
    p1 = df$ctr[1],
    p2 = df$ctr[2],
    sig.level = 0.05,
    alternative = "two.sided"
  )

autovitData <- df
autovitTestResults <- testResults

# standvirtual

df <-
  data.frame(
    id = c("", ""),
    name = c("Grey Button", "Blue Button"),
    period = c("Dec 17, 2017 - Jan 07, 2018", "Dec 17, 2017 - Jan 07, 2018"),
    PopulationMetric = c("my_ads_active","my_ads_active"),
    PopulationValue = c( 2159, 2194),
    SucessMetric = c("promote_in_olx","promote_in_olx"),
    SucessValue = c(27, 42)
  )

df$ctr <- 
  df$SucessValue / df$PopulationValue

testResults <- 
  power.prop.test(
    n = df$PopulationValue, 
    p1 = df$ctr[1],
    p2 = df$ctr[2],
    sig.level = 0.05,
    alternative = "two.sided"
  )

standvirtualData <- df
standvirtualTestResults <- testResults