require("data.table")
require("dplyr")
require("dtplyr")
require("feather")
require("fasttime")
library("magrittr")

files <- list.files(path = '/home/daniel.rocha/datalake/',
                    pattern = paste0('^RDL.*', 'OtomotoPL','.*','ads' ,'.*feather$'),
                    full.names = TRUE
)
  
  system.time({
  dat_list = lapply(files, function (x) read_feather((x)))
  }
  )
  
system.time({
dat = rbindlist(dat_list, fill = TRUE)
}
)

system.time({
df <-
  dat %>%
  mutate(created_at_first = fastPOSIXct(created_at_first),
         created_at_first_hour = hour(created_at_first),
         created_at_first_day = as.Date(created_at_first)) %>%
  group_by(created_at_first_day, created_at_first_hour) %>%
  summarise(qtyNNL = sum(n()))
}
)