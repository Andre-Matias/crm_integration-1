# libraries -------------------------------------------------------------------
library("config")
library("fasttime")
library("data.table")
library("dplyr")
library("dtplyr")
library("magrittr")
library("ggplot2")
library("stringr")
library("ggthemes")
library("scales")

# load db configurations ------------------------------------------------------
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "standvirtual_pt") )

# -----------------------------------------------------------------------------
load("~/credentials.Rdata")

# get data

library("RMySQL")

conDB<- 
  dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= bi_team_pt_password,  
                  host = "127.0.0.1", 
                  port = as.numeric(config$BiServerPort),
                  dbname = config$DbName
            )



cmdSqlQuery <- 
  "
    SELECT *
    FROM carspt.ads
    WHERE
      category_id = 661
      AND status = 'active'
    ;
  "

dfQueryResults <- dbGetQuery(conDB,cmdSqlQuery)

dbDisconnect(conDB)

dfAds <- dfQueryResults

rm("dfQueryResults")

# extract category and subcategory from params field --------------------------

dfAds$category <-
  unlist(
  lapply(dfAds$params,
         function (x) str_match(x, "category\\<\\=\\>(.*?)\\<br\\>")[1,2]
         )
  )

dfAds$subcategory <- 
  unlist(
  lapply(dfAds$params,
         function (x) str_match(x, "sub\\_category\\<\\=\\>(.*?)\\<br\\>")[1,2]
         )
  )

dfAds$type <- 
  unlist(
    lapply(dfAds$params,
           function (x) str_match(x, "type\\<\\=\\>(.*?)\\<br\\>")[1,2]
    )
  )

# -----------------------------------------------------------------------------

dfAds$category[is.na(dfAds$category) |
                 dfAds$category == ""] <- "BLANK"

dfAds$subcategory[is.na(dfAds$subcategory) | 
                    dfAds$subcategory == "" |
                    dfAds$subcategory == "blank" ] <- "BLANK"

dfAds$type[is.na(dfAds$type) | 
                    dfAds$type == "" |
                    dfAds$type == "blank" ] <- "BLANK"

# Stats for category field ----------------------------------------------------

dfCategoryStats <-
  dfAds %>%
  group_by(category) %>%
  summarise(
    qtyAds = sum(n())
  ) %>%
  mutate(
    perAds = qtyAds / sum(qtyAds)
  ) %>%
  arrange(desc(category))

# Stats for subcategory field -------------------------------------------------

dfSubCategoryStats <-
  dfAds %>%
  group_by(subcategory) %>%
  summarise(
    qtyAds = sum(n())
  ) %>%
  mutate(
    perAds = qtyAds / sum(qtyAds)
  ) %>%
  arrange(desc(subcategory))

# Stats for subcategory field -------------------------------------------------

dfTypeStats <-
  dfAds %>%
  group_by(type) %>%
  summarise(
    qtyAds = sum(n())
  ) %>%
  mutate(
    perAds = qtyAds / sum(qtyAds)
  ) %>%
  arrange(desc(type))

# graph for category field ----------------------------------------------------

ghCategory <-
  ggplot(dfCategoryStats)+
  geom_bar(stat = "identity", aes(category, perAds))+
  geom_text(aes(x= category, y = perAds,
                label = paste0(percent(round(perAds, 2)), " (", qtyAds,")")),
            hjust = - 0.1)+ 
  coord_flip()+
  scale_y_continuous(labels = percent, limits = c(0, 0.4))+
  theme_fivethirtyeight()+
  ggtitle("Percentage of Parts Listings with Blank Category")

# graph for subcategory field -------------------------------------------------

ghSubCategory <-
  ggplot(dfSubCategoryStats)+
  geom_bar(stat = "identity", aes(subcategory, perAds))+
  geom_text(aes(x= subcategory, y = perAds,
                label = paste0(percent(round(perAds, 2)), " (", qtyAds,")")),
            hjust = - 0.1)+ 
  coord_flip()+
  scale_y_continuous(labels = percent, limits = c(0, 1))+
  theme_fivethirtyeight()+
  ggtitle("Percentage of Parts Listings with Blank SubCategory")

# graph for type field --------------------------------------------------------

ghType <-
  ggplot(dfTypeStats)+
  geom_bar(stat = "identity", aes(type, perAds))+
  geom_text(aes(x= type, y = perAds,
                label = paste0(percent(round(perAds, 2)), " (", qtyAds,")")),
            hjust = - 0.1)+ 
  coord_flip()+
  scale_y_continuous(labels = percent, limits = c(0, 1))+
  theme_fivethirtyeight()+
  ggtitle("Percentage of Parts Listings with Blank Type")

