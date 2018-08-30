#patchBodyTypes <-
#  function(df){
#
dfRules <-
read.table("~/ixalan/CarsValuation/transform_segments.tsv", sep="\t", header = TRUE)

df <- head(readRDS("~/tmp/RawHistoricalAds_OTO_main_AIO_wPrice.RDS"), 10000)

# backup

df$old_body_type <- df$body_type
dfRules$filters <- paste0("<br>", dfRules$filters, "<br>")

dfRules$make <- str_extract_all(dfRules$filters, "<br>make<=>(.*?)<br>")
dfRules$make <- gsub("<br>make<=>","", dfRules$make)
dfRules$make <- gsub("<br>","", dfRules$make)


dfRules$model <- str_extract_all(dfRules$filters, "<br>model<=>(.*?)<br>")
dfRules$model <- gsub("<br>model<=>","", dfRules$model)
dfRules$model <- gsub("<br>","", dfRules$model)

dfRules$body_type <- str_extract_all(dfRules$filters, "<br>body_type<=>(.*?)<br>")
dfRules$body_type <- gsub("<br>body_type<=>","", dfRules$body_type)
dfRules$body_type <- gsub("<br>","", dfRules$body_type)

dfRules$values_to_update <- gsub("body_type<=>","", dfRules$values_to_update)

df$new_segment <- ""

df$make <- as.character(df$make)
df$model <- as.character(df$model)
df$body_type <- as.character(df$body_type)

df <- as.data.table(df)

df1 <- as.data.table(
  dfRules[
    grepl("make",dfRules$filters) &
    grepl("model",dfRules$filters) &
    grepl("body_type",dfRules$filters), 
    c("id", "make", "model", "body_type", "values_to_update")
  ]
)

df1_slice <-
  df %>%
  filter(new_segment == "") %>%
  inner_join(df1, by = c("make"="make", "model"="model", "body_type"="body_type")) %>%
  select(-body_type, -make, model) %>%
  select(ad_id, old_body_type, values_to_update)

df2 <-
  as.data.table(
  dfRules[
    grepl("make",dfRules$filters) &
    grepl("model",dfRules$filters) &
    !(dfRules$id %in% df1$id), 
    c("id", "make", "model", "body_type", "values_to_update")
    ]
  )

df2_slice <-
  df %>%
  filter(new_segment == "" 
         & !(ad_id %in% df1_slice$ad_id)
         ) %>%
  inner_join(df2, by = c("make"="make", "model"="model")) %>%
  select(-body_type.x, -body_type.y, -make, model) %>%
  select(ad_id, old_body_type, values_to_update)


df3 <-
  as.data.table(
  dfRules[
    grepl("model",dfRules$filters) &
    grepl("body_type",dfRules$filters) &
    !(dfRules$id %in% df1$id) &
    !(dfRules$id %in% df2$id),
    c("id", "make", "model", "body_type", "values_to_update")
    ]
  )

df3_slice <-
  df %>%
  filter(new_segment == "" 
         & !(ad_id %in% df1_slice$ad_id)
         & !(ad_id %in% df2_slice$ad_id)
  ) %>%
  inner_join(df3, by = c("model"="model", "body_type"="body_type")) %>%
  select(-body_type, -make.x, -make.y, model) %>%
  select(ad_id, old_body_type, values_to_update)
  



df4 <- 
  as.data.table(
    dfRules[
      !(dfRules$id %in% df1$id)
      & !(dfRules$id %in% df2$id)
      & !(dfRules$id %in% df3$id) 
      & grepl("model",dfRules$filters),
      c("id", "make", "model", "body_type", "values_to_update")
      ]
  )

df4_slice <-
  df %>%
  filter(new_segment == "" 
         & !(ad_id %in% df1_slice$ad_id)
         & !(ad_id %in% df2_slice$ad_id)
         & !(ad_id %in% df3_slice$ad_id)
  ) %>%
  inner_join(df4, by = c("model"="model")) %>%
  select(-body_type.x, -make.x, -make.y, -body_type.y, model) %>%
  select(ad_id, old_body_type, values_to_update)

df5<- 
  as.data.table(
    dfRules[
      !(dfRules$id %in% df1$id)
      & !(dfRules$id %in% df2$id)
      & !(dfRules$id %in% df3$id)
      & !(dfRules$id %in% df4$id)
      & grepl("body_type",dfRules$filters),
      c("id", "make", "model", "body_type", "values_to_update")
      ]
  )

df5_slice <-
  df %>%
  filter(new_segment == "" 
         & !(ad_id %in% df1_slice$ad_id)
         & !(ad_id %in% df2_slice$ad_id)
         & !(ad_id %in% df3_slice$ad_id)
         & !(ad_id %in% df4_slice$ad_id)
  ) %>%
  inner_join(df5, by = c("body_type"="body_type")) %>%
  select(-body_type, -make.x, model.x, -make.y, -model.y) %>%
  select(ad_id, old_body_type, values_to_update)

df6 <- 
  as.data.table(
    dfRules[
      !(dfRules$id %in% df1$id)
      & !(dfRules$id %in% df2$id)
      & !(dfRules$id %in% df3$id)
      & !(dfRules$id %in% df4$id)
      & !(dfRules$id %in% df5$id)
      , ]
  )

f <- rbind(df1_slice,
           df2_slice,
           df3_slice,
           df4_slice,
           df5_slice
           )

#}

#Atenção à rule combi