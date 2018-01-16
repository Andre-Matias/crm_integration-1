# #################################################################################################
# Functions to calculate population and success parameters                                        #
# to be needed for performing power statistical tests                                             #
#                                                                                                 #
# (could optimize them and use the same one adding extra argument for OKR1/OKR2 days selection)   #
# #################################################################################################


#' pop_suc_okr1() function:
#' Takes a OKR1 retention dataset ("df_any_..") and returns population and successes 
#' population and success parameter are needed as inputs to evaluate test significance with power analysis
## need to define timerange and platform within the function
pop_suc_okr1 <- function (df_any, from, to, plat) {
  # start_date, end_date have to be passed with format "2017-11-22" between double quotes
  # platform has to be passed like "rwd" between double quotes
  clean <- df_any %>%
    select(dates:retainCount.7, platform) %>%
    select(-retainCount.0)
  
  row.names(clean) <- NULL
  my_interval <- as.character(seq(1,7))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  clean$Date <- as.Date(as.character(clean$Date))
  # clean <- clean %>%
  #   filter (Date >= from & Date <= to & platform= platform)
  clean <- subset (clean, Date >= from & Date <= to & platform== plat )
  clean <- clean[, 1:9]
  clean$success <- rowSums(clean[,3:9])
  pop <- sum(clean$TotalUsers)
  suc <- sum(clean$success)
  ret <- suc / pop
  # Put population and successes into a data frame for convenience
  pop_suc <- data.frame (pop, suc, ret)
  return (pop_suc)
}

#' pop_suc_okr2() function:
#' Returns for population and successes for OKR2 retention dataset
pop_suc_okr2 <- function (df_lead, from, to, plat) {
  # start_date, end_date have to be passed with format "2017-11-22" between double quotes
  # platform has to be passed like "rwd" between double quotes
  clean <- df_lead %>%
    select(dates:retainCount.14, platform)
  
  row.names(clean) <- NULL
  my_interval <- as.character(seq(0,14))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  clean$Date <- as.Date(as.character(clean$Date))

  clean <- subset (clean, Date >= from & Date <= to & platform== plat )
  clean <- clean[, 1:17]
  clean$success <- rowSums(clean[,3:17])
  pop <- sum(clean$TotalUsers)
  suc <- sum(clean$success)
  ret <- suc / pop
  # Put population and successes into a data frame for convenience
  pop_suc <- data.frame (pop, suc, ret)
  return (pop_suc)
}

#' pop_suc_okr2() function:
#' Returns for population and successes for OKR2 retention dataset, only for day zero
pop_suc_okr2_0 <- function (df_lead, from, to, plat) {
  # start_date, end_date have to be passed with format "2017-11-22" between double quotes
  # platform has to be passed like "rwd" between double quotes
  clean <- df_lead %>%
    select(dates:retainCount.0, platform)
  
  names(clean) <- c("Date", "TotalUsers", "same_day", "platform")
  clean$Date <- as.Date(as.character(clean$Date))
  
  clean <- subset (clean, Date >= from & Date <= to & platform== plat )
  clean <- clean[, 1:3]
  pop <- sum(clean$TotalUsers)
  suc <- sum(clean$same_day)
  ret <- suc / pop
  # Put population and successes into a data frame for convenience
  pop_suc <- data.frame (pop, suc, ret)
  return (pop_suc)
}


#' pop_suc_okr2() function:
#' Returns for population and successes for OKR2 retention dataset, only for days 1-14 
pop_suc_okr2_1_14 <- function (df_lead, from, to, plat) {
  # start_date, end_date have to be passed with format "2017-11-22" between double quotes
  # platform has to be passed like "rwd" between double quotes
  clean <- df_lead %>%
    select(dates:retainCount.14, platform)%>%
    select(-retainCount.0)
  
  row.names(clean) <- NULL
  my_interval <- as.character(seq(1,14))
  names(clean) <- c("Date", "TotalUsers", my_interval, "platform")
  clean$Date <- as.Date(as.character(clean$Date))
  
  clean <- subset (clean, Date >= from & Date <= to & platform== plat )
  clean <- clean[, 1:16]
  clean$success <- rowSums(clean[,3:16])
  pop <- sum(clean$TotalUsers)
  suc <- sum(clean$success)
  ret <- suc / pop
  # Put population and successes into a data frame for convenience
  pop_suc <- data.frame (pop, suc, ret)
  return (pop_suc)
}


  
# Run function
# control_okr2 <- pop_suc_okr2(df_lead_pl, "2017-11-22", "2017-11-26", "rwd")
# experiment_okr2 <- pop_suc_okr2 (df_lead_pl_pwa, "2017-11-22", "2017-11-26", "rwd")

  