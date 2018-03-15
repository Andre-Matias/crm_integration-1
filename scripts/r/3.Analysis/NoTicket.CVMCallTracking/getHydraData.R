# load libraries --------------------------------------------------------------
library("RMixpanel")
library("dplyr")
library("data.table")
library("dtplyr")
library("magrittr")
library("tidyr")
library("anytime")
library("ggthemes")
library("showtext")
library("RPostgreSQL")

# load mixpanel user's credentials --------------------------------------------
load("~/credentials.Rdata")
font_add_google("Open Sans", "opensans")

# 
startDate <- as.POSIXct("2018-03-10")
endDate <- as.POSIXct("2018-03-11")

dates <-
  seq(startDate, endDate, "day")

events <-
  as.character(
  paste(
  c(
    'reply_message_form_click',
    'reply_message_email',
    'reply_phone_1step',
    'reply_message_click',
    'reply_phone_call',
    'reply_phone_show',
    'reply_message_sent',
    'reply_phone_sms',
    'reply_phone_create_contact',
    'reply_phone_copy_number',
    'reply_phone_cancel',
    'reply_chat_sent',
    'reply_chat_update',
    'reply_chat_click'
  ),
  collapse = ","
)
)

listServerPath <-
  list(
    otomoto = c('/h/v-otomoto-android', '/h/v-otomoto-ios', '/h/v-otomoto-web')
    #autovit = c('/h/v-autovit-android', '/h/v-autovit-ios', '/h/v-autovit-web'),
    #standvirtual = c('/h/v-standvirtual-android', '/h/v-standvirtual-ios', '/h/v-standvirtual-web'),
    #otodom = c('/h/v-otodom-android', '/h/v-otodom-ios', '/h/v-otodom-web'),
    #imovirtual = c('/h/v-imovirtual-android', '/h/v-imovirtual-ios', '/h/v-imovirtual-web')
    #storia = c('/h/v-storia-android', '/h/v-storia-ios', "/h/v-storia-web")
  )
for(date in dates){

  epochDate <- as.integer(date)
  monthText <- as.character(format(as.Date(date, origin = '1970-01-01'), "%Y%m"))
  
for(server in listServerPath){

  server_path <- as.character(server[1])
  print(server[1])
  print()
androidQuery <- 
  glue(
    "SELECT * FROM hydra.verticals_ninja_android_{monthText}
    WHERE server_path = '{server}'
    AND eventname IN ({events})
    ")

print(androidQuery)
}
}

# connect to Triton Silver ----------------------------------------------------

 drv <- dbDriver("PostgreSQL")

 conDB <-
   dbConnect(
     drv,
     host = dwTritonSilverDbHost,
     port = dwTritonSilverDbPort,
     dbname = "olxgroupbi",
     user = myTritonUser,
     password = myTritonPass
   )

 requestDB <-
   dbSendQuery(
     conDB, sqlQuery
     )

 dfRequestDB <- dbFetch(requestDB)
 dbClearResult(dbListResults(conDB)[[1]])

dfHydraReplies <- dfRequestDB

dbDisconnect(conDB)
}
}

