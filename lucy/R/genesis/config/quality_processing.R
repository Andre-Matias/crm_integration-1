raw_tables <- ls()[c(grep('_gDf',ls()))]

# Processing percentages and rates on properties

for (i in raw_tables){
  
  # create variables and tables
  table <- eval(parse(text = i))
  table_name_pct <- paste0(strsplit(i,'_gDf')[[1]][1],'_pct')
  table_name_rates <- paste0(strsplit(i,'_gDf')[[1]][1],'_rates')
  reference <- table %>% select(name,hits)
  percentage <- table %>% select(-name,-hits)
  columns <- colnames(percentage)
  
  # working on percentages
  for (c in columns){
    percentage[c] <- round(percentage[c] / reference['hits'] * 100,2)
  }
  
  # working on rating
  rates <- percentage
  denom <- 0
  for (c in columns){
    split <- strsplit(c,'___')[[1]][2]
    
    # rating each property based on the percentage amount of bugs
    rates[c] <- as.integer(
      ifelse(rates[c] < 1,5,
      ifelse(rates[c] < 3,4,
      ifelse(rates[c] < 5,3,
      ifelse(rates[c] < 10,2,1)))))
    
    # setting up the importance degree
    if (split == 'na'){
      bareme <- 3
    } else if (split == 'val'){
      bareme <- 3
    } else if (split == 'duplicates'){
      bareme <- 3
    } else if (split == 'alloc'){
      bareme <- 2
    } else if (split == 'parent'){
      bareme <- 2
    } else if (split == 'noise'){
      bareme <- 1
    }
    
    rates[c] <- rates[c] * bareme
    denom <- denom + bareme
  }
  
  # update the tables
  #percentage <- cbind(event = reference['name'],hits = reference['hits'],percentage)
  #rates <- cbind(event = reference['name'],hits = reference['hits'],rates)
  
  # final dataframes
  #assign(table_name_pct,percentage)
  #assign(table_name_rates,rates)
}

percentage_tables <- raw_tables <- ls()[c(grep('_pct',ls()))]
rate_tables <- ls()[c(grep('_rates',ls()))]

