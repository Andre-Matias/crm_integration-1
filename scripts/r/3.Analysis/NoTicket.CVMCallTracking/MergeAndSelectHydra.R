# load libraries
library("data.table")
library("dplyr")
library("dtplyr")
library("tidyr")
library("anytime")

# lists ads files from datalake -----------------------------------------------
files <-
  list.files(path = '~/CT', pattern = 'v-otodom.*',
             full.names = TRUE)

# read all files to a list ----------------------------------------------------
dat_list <-
  lapply(files, function (x){
    print(x)
    data.table(readRDS(x))
  }
  )

# merge all data frames from the list to a single data frame ------------------
dat <-
  as_tibble(rbindlist(dat_list, use.names = TRUE, fill = TRUE))


#------------------------------------------------------------------------------

dfLeadsByAd <-
  dat %>%
  filter(!is.na(platform_type))%>%
  select(server_date_trunc, item_id, eventname) %>%
  mutate(date = as.Date(as.POSIXct(server_date_trunc, origin="1970-01-01")))%>%
  group_by(item_id, date, eventname) %>%
  summarise(qtyEvents = sum(n())) #%>%
  #spread(key = eventname, value = qtyEvents, fill = 0)
  