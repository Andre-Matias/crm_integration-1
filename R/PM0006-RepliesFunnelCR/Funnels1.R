

library(RMixpanel)
library(openxlsx)
library(sqldf)
library(RCurl)
library("jsonlite")
library("stringr")
library("tidyr")
library("magrittr")
library(formattable)
library(dplyr)
library(formattable)


# Create accounts. 
--------------------------------------------------------------------------------
  
  
account_imo <- mixpanelCreateAccount("imovirtual.pt",
                                      token="fbcae190c2396b3f725856d427c197d0",
                                      secret="1103d4986633d8967760948cac3002ca", 
                                      key="494bb6c4faaccfa392e4dc1f72c97d54")



account_stro = mixpanelCreateAccount("storia.ro",
                                     token="6900af9e71311749fef8ca611dab940e",
                                     secret="d317a9cc40cd231587ce92b443f2ca44", 
                                     key="a3d12ec5d6428aa26aaaaa33ff2e7688")




account_otpl = mixpanelCreateAccount("otodom.pl",
                                     token="5da98ecd30a0b9103c5c42f2d2c5575b",
                                     secret="f11909a90782f605aef692025f648546", 
                                     key="12877dfd1d62b1f6a69ed910e91d248a")



--------------
  

  ## Get list of funnels.
  mixpanelGetData(account_imo, method="funnels/list/", args=list(), data=TRUE)
## Example output:
## [1] "[{\"funnel_id\": 1011888, \"name\": \"My first funnel\"}, 
##       {\"funnel_id\": 1027999, \"name\": \"User journey funnel\"}]"

## Get data about a certain funnel.

#Imovirtual 
funnel_home_imo1 <- (mixpanelGetData(account_imo, method = "funnels/", args = list(funnel_id="2909853", unit="week"), 
                  data = TRUE))
funnel_home_imo <- as.data.frame(fromJSON(funnel_home_imo1, flatten = TRUE)$data)

funnel_list_imo1 <- (mixpanelGetData(account_imo, method = "funnels/", args = list(funnel_id="2909969", unit="week"), 
                                     data = TRUE))
funnel_list_imo <- as.data.frame(fromJSON(funnel_list_imo1, flatten = TRUE)$data)

funnel_adpage_imo1 <- (mixpanelGetData(account_imo, method = "funnels/", args = list(funnel_id="2909725", unit="week"), 
                                     data = TRUE))
funnel_adpage_imo <- as.data.frame(fromJSON(funnel_adpage_imo1, flatten = TRUE)$data)


#Otodom PL 
funnel_home_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="2915233", unit="week"), 
                                       data = TRUE))
funnel_home_otpl <- as.data.frame(fromJSON(funnel_home_otpl1, flatten = TRUE)$data)

funnel_list_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="2910541", unit="week"), 
                                      data = TRUE))
funnel_list_otpl <- as.data.frame(fromJSON(funnel_list_otpl1, flatten = TRUE)$data)

funnel_adpage_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="2915109", unit="week"), 
                                      data = TRUE))
funnel_adpage_otpl <- as.data.frame(fromJSON(funnel_adpage_otpl1, flatten = TRUE)$data)

#Storia RO
funnel_home_stro1 <- (mixpanelGetData(account_stro, method = "funnels/", args = list(funnel_id="2915405",unit="week"), 
                                       data = TRUE))
funnel_home_stro <- as.data.frame(fromJSON(funnel_home_stro1, flatten = TRUE)$data)


funnel_list_stro1 <- (mixpanelGetData(account_stro, method = "funnels/", args = list(funnel_id="2915349",unit="week"), 
                                      data = TRUE))
funnel_list_stro <- as.data.frame(fromJSON(funnel_list_stro1, flatten = TRUE)$data)


funnel_adpage_stro1 <- (mixpanelGetData(account_stro, method = "funnels/", args = list(funnel_id="2915313",unit="week"), 
                                      data = TRUE))
funnel_adpage_stro <- as.data.frame(fromJSON(funnel_adpage_stro1, flatten = TRUE)$data)


#Pre Processing Otodom PL 

funnel_home_otpl <- funnel_home_otpl[c(22,23,24,25)]
colnames(funnel_home_otpl) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_home_otpl$origin <- c("home_","home","search","listing.","adpage_")
funnel_home_otpl$to <- c("home","search","listing.","adpage_","replies_home")
funnel_home_otpl <- funnel_home_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_list_otpl <- funnel_list_otpl[c(13,14,15,16)]
colnames(funnel_list_otpl) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_list_otpl$origin <- c("listing_","listing",".adpage")
funnel_list_otpl$to <- c("listing",".adpage","replies_listing")
funnel_list_otpl <- funnel_list_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_adpage_otpl <- funnel_adpage_otpl[c(13,14,15,16)]
colnames(funnel_adpage_otpl) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_adpage_otpl$origin <- c("adpage.","adpage")
funnel_adpage_otpl$to <- c("adpage","replies_adpage")
funnel_adpage_otpl <- funnel_adpage_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]

funnel_total_otpl <- rbind (funnel_home_otpl,funnel_list_otpl,funnel_adpage_otpl)

funnel_total_otpl_per <- rbind (funnel_home_otpl,funnel_list_otpl,funnel_adpage_otpl)
funnel_total_otpl_per$"Overall Conversion" <- percent(funnel_total_otpl$"Overall Conversion")
funnel_total_otpl_per$"Step conversion" <- percent(funnel_total_otpl$"Step conversion")

save(funnel_total_otpl,file="funnel_total_otpl.RData")
save(funnel_total_otpl_per,file="funnel_total_otpl_per.RData")


#Data Transformation Storia Ro 
funnel_home_stro <- funnel_home_stro[c(22,23,24,25)]
colnames(funnel_home_stro) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_home_stro$origin <- c("home_","home","search","listing.","adpage_")
funnel_home_stro$to <- c("home","search","listing.","adpage_","replies_home")
funnel_home_stro <- funnel_home_stro[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_list_stro <- funnel_list_stro[c(13,14,15,16)]
colnames(funnel_list_stro) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_list_stro$origin <- c("listing_","listing",".adpage")
funnel_list_stro$to <- c("listing",".adpage","replies_listing")
funnel_list_stro <- funnel_list_stro[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_adpage_stro <- funnel_adpage_stro[c(13,14,15,16)]
colnames(funnel_adpage_stro) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_adpage_stro$origin <- c("adpage.","adpage")
funnel_adpage_stro$to <- c("adpage","replies_adpage")
funnel_adpage_stro <- funnel_adpage_stro[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_total_stro <- rbind (funnel_home_stro,funnel_list_stro,funnel_adpage_stro)
funnel_total_stro_per <- rbind (funnel_home_stro,funnel_list_stro,funnel_adpage_stro)

funnel_total_stro_per$"Overall Conversion" <- percent(funnel_total_stro$"Overall Conversion")
funnel_total_stro_per$"Step conversion" <- percent(funnel_total_stro$"Step conversion")

save(funnel_total_stro,file="funnel_total_stro.RData")
save(funnel_total_stro_per,file="funnel_total_stro_per.RData")

#funnel_list_stro$Step <- as.character(funnel_list_stro$Step)
#funnel_list_stro$Step <- gsub(".*event.*", "replies", funnel_home_stro$Step)


#options(digits=2)
#funnel_home_stro$`Step conversion` <- as.numeric(funnel_home_stro$`Step conversion`)


#funnel_home_stro$`Step conversion` <- percent(funnel_home_stro$`Step conversion`)


#Imovirtual Data Transformation 

funnel_home_imo <- funnel_home_imo[c(22,23,24,25)]
colnames(funnel_home_imo) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_home_imo$origin <- c("home_","home","search","listing.","adpage_")
funnel_home_imo$to <- c("home","search","listing.","adpage_","replies_home")
funnel_home_imo <- funnel_home_imo[,c("origin","to","Values","Step conversion","Overall Conversion")]



funnel_list_imo <- funnel_list_imo[c(13,14,15,16)]
colnames(funnel_list_imo) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_list_imo$origin <- c("listing_","listing",".adpage")
funnel_list_imo$to <- c("listing",".adpage","replies_listing")
funnel_list_imo <- funnel_list_imo[,c("origin","to","Values","Step conversion","Overall Conversion")]


funnel_adpage_imo <- funnel_adpage_imo[c(13,14,15,16)]
colnames(funnel_adpage_imo) <- c("Values","Step conversion","Step","Overall Conversion")
funnel_adpage_imo$origin <- c("adpage.","adpage")
funnel_adpage_imo$to <- c("adpage","replies_adpage")
funnel_adpage_imo <- funnel_adpage_imo[,c("origin","to","Values","Step conversion","Overall Conversion")]

funnel_total_imo <- rbind (funnel_home_imo,funnel_list_imo,funnel_adpage_imo)


funnel_total_imo_per <- rbind (funnel_home_imo,funnel_list_imo,funnel_adpage_imo)
funnel_total_imo_per$"Overall Conversion" <- percent(funnel_total_imo$"Overall Conversion")
funnel_total_imo_per$"Step conversion" <- percent(funnel_total_imo$"Step conversion")

save(funnel_total_imo,file="funnel_total_imo.RData")
save(funnel_total_imo_per,file="funnel_total_imo_per.RData")



#Sankey Visualisation 

# #Storia RO 
# colors_link <- c('green','blue',"yellow")
# colors_link_array <- paste0("[", paste0("'", colors_link,"'", collapse = ','), "]")
# colors_node <- c('black','orange')
# colors_node_array <- paste0("[", paste0("'", colors_node,"'", collapse = ','), "]")
# opts <- paste0("{
#         link: { colorMode: 'source',
#                colors: ", colors_link_array ," },
#                node: { colors: ", colors_node_array ," }
#                }" )


require(googleVis)
plot(
  gvisSankey(funnel_total_stro, from="origin", 
             to="to",weight="Step conversion",
             options=list(
               height=700,width=1200,
               sankey="{link: {color: { fill: 'lightblue' } },
                        node: { width: 30, 
               color: { fill: '#a61d4c' },
               label: { fontName: 'San Serif',
               fontSize: 15,
               color: 'black',
               bold: false,
               italic: true } }}"))
      
)



require(googleVis)
plot(
  gvisSankey(funnel_total_imo, from="origin", 
             to="to",weight="Step conversion",
             options=list(
               height=700,width=1200,
               sankey="{link: {color: { fill: 'lightblue' } },
               node: { width: 30, 
               color: { fill: '#a61d4c' },
               label: { fontName: 'San Serif',
               fontSize: 15,
               color: 'black',
               bold: false,
               italic: true } }}"))
  
             )



require(googleVis)
plot(
  gvisSankey(funnel_total_otpl, from="origin", 
             to="to",weight="Step conversion",
             options=list(
               height=700,width=1200,
               sankey="{link: {color: { fill: 'lightblue' } },
               node: { width: 30, 
               color: { fill: '#a61d4c' },
               label: { fontName: 'San Serif',
               fontSize: 15,
               color: 'black',
               bold: false,
               italic: true } }}"))
  
             )










