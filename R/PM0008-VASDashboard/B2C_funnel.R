
library(RMixpanel)
library(sqldf)
library(RCurl)
library("jsonlite")
library("stringr")
library("tidyr")
library("magrittr")
library(formattable)
library(dplyr)
library(data.table)





# Create accounts. 
--------------------------------------------------------------------------------
  
  
  
  account_otpl = mixpanelCreateAccount("otodom.pl",
                                       token="5da98ecd30a0b9103c5c42f2d2c5575b",
                                       secret="f11909a90782f605aef692025f648546", 
                                       key="12877dfd1d62b1f6a69ed910e91d248a")
  
  
  
  
  account_imo <- mixpanelCreateAccount("imovirtual.pt",
                                       token="fbcae190c2396b3f725856d427c197d0",
                                       secret="1103d4986633d8967760948cac3002ca", 
                                       key="494bb6c4faaccfa392e4dc1f72c97d54")
  
  
  
  account_stro = mixpanelCreateAccount("storia.ro",
                                       token="6900af9e71311749fef8ca611dab940e",
                                       secret="d317a9cc40cd231587ce92b443f2ca44", 
                                       key="a3d12ec5d6428aa26aaaaa33ff2e7688")
  
  
  
  --------------
    
    
    ## Get list of funnels.
    # mixpanelGetData(account_imo, method="funnels/list/", args=list(), data=TRUE)
    ## Example output:
    ## [1] "[{\"funnel_id\": 1011888, \"name\": \"My first funnel\"}, 
    ##       {\"funnel_id\": 1027999, \"name\": \"User journey funnel\"}]"
    
    ## Get data about a certain funnel.
    
    
  #Otodom PL
  
  funnel_b2c_payment_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="3188369",length=7,length_unit='day',unit="week"), 
                                               data = TRUE))
  funnel_b2c_payment_otpl <- as.data.frame(fromJSON(funnel_b2c_payment_otpl1, flatten = TRUE)$data)
  
  
  #Pre Processing Otodom PL
  
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[c(1,2,3,4)]
  colnames(funnel_b2c_payment_otpl) <- c("Values","Step","Overall Conversion","Step conversion")
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_b2c_payment_otpl$origin <- c("myaccount_","myaccount","multipay_page")
  funnel_b2c_payment_otpl$to <- c("myaccount","multipay_page","multipay_finished")
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  
  save(funnel_b2c_payment_otpl,file='funnel_b2c_payment_otpl.RData')
  
  load("funnel_b2c_payment_otpl.RData")


# Imovirtual PT 
  
  funnel_b2c_payment_imo1 <- (mixpanelGetData(account_imo, method = "funnels/", args = list(funnel_id="3443109",length=7,length_unit='day',unit="week"), 
                                               data = TRUE))
  funnel_b2c_payment_imo <- as.data.frame(fromJSON(funnel_b2c_payment_imo1, flatten = TRUE)$data)
  
  
  #Pre Processing Otodom PL
  
  funnel_b2c_payment_imo <- funnel_b2c_payment_imo[c(1,2,3,4)]
  colnames(funnel_b2c_payment_imo) <- c("Values","Step","Overall Conversion","Step conversion")
  funnel_b2c_payment_imo <- funnel_b2c_payment_imo[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_b2c_payment_imo$origin <- c("myaccount_","myaccount","multipay_page")
  funnel_b2c_payment_imo$to <- c("myaccount","multipay_page","multipay_finished")
  funnel_b2c_payment_imo <- funnel_b2c_payment_imo[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  
  save(funnel_b2c_payment_imo,file='funnel_b2c_payment_imo.RData')
  
  load("funnel_b2c_payment_imo.RData")
  
  
  
  # StoriaRo
  
  funnel_b2c_payment_str1 <- (mixpanelGetData(account_stro, method = "funnels/", args = list(funnel_id="3443285",length=7,length_unit='day',unit="week"), 
                                               data = TRUE))
  funnel_b2c_payment_str <- as.data.frame(fromJSON(funnel_b2c_payment_str1, flatten = TRUE)$data)
  
  
  #Pre Processing Otodom PL
  
  funnel_b2c_payment_str <- funnel_b2c_payment_str[c(1,2,3,4)]
  colnames(funnel_b2c_payment_str) <- c("Values","Step","Overall Conversion","Step conversion")
  funnel_b2c_payment_str <- funnel_b2c_payment_str[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_b2c_payment_str$origin <- c("myaccount_","myaccount","multipay_page")
  funnel_b2c_payment_str$to <- c("myaccount","multipay_page","multipay_finished")
  funnel_b2c_payment_str <- funnel_b2c_payment_str[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  
  save(funnel_b2c_payment_str,file='funnel_b2c_payment_str.RData')
  
  load("funnel_b2c_payment_str.RData")
  
  
  
  --------------------------------------  --------------------------------------  --------------------------------------  --------------------------------------

 
  
  
  require(googleVis)
  plot(
    gvisSankey(funnel_b2c_payment_otpl, from="origin", 
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
  
  
  
  
  
  