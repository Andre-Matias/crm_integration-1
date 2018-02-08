
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
  
  --------------
    
    
    ## Get list of funnels.
    # mixpanelGetData(account_imo, method="funnels/list/", args=list(), data=TRUE)
    ## Example output:
    ## [1] "[{\"funnel_id\": 1011888, \"name\": \"My first funnel\"}, 
    ##       {\"funnel_id\": 1027999, \"name\": \"User journey funnel\"}]"
    
    ## Get data about a certain funnel.
    
    
  #Otodom PL 
  funnel_global_payment_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id=" 3164353",unit="week"), 
                                                  data = TRUE))
  funnel_global_payment_otpl <- as.data.frame(fromJSON(funnel_global_payment_otpl1, flatten = TRUE)$data)
  
  funnel_b2c_payment_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="3188369",from_date="2018-01-28",to_date="2018-02-04",unit="week"), 
                                               data = TRUE))
  funnel_b2c_payment_otpl <- as.data.frame(fromJSON(funnel_b2c_payment_otpl1, flatten = TRUE)$data)
  
  funnel_trans_otpl1 <- (mixpanelGetData(account_otpl, method = "funnels/", args = list(funnel_id="3188417",from_date="2018-01-28",to_date="2018-02-04",unit="week"),
                                         data = TRUE))
  funnel_trans_otpl <- as.data.frame(fromJSON(funnel_trans_otpl1, flatten = TRUE)$data)
  
  
  #Pre Processing Otodom PL 
  
  funnel_global_payment_otpl <- funnel_global_payment_otpl[c(11,12,13,14)]
  colnames(funnel_global_payment_otpl) <- c("Values","Step conversion","Step","Overall Conversion")
  funnel_global_payment_otpl <- funnel_global_payment_otpl[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_global_payment_otpl$origin <- c("myaccount_","myaccount","multipay_page","multipay_finished")
  funnel_global_payment_otpl$to <- c("myaccount","multipay_page","multipay_finished","multipay_confirmation_page")
  funnel_global_payment_otpl <- funnel_global_payment_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[c(1,3,4,6)]
  colnames(funnel_b2c_payment_otpl) <- c("Values","Step","Overall Conversion","Step conversion")
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_b2c_payment_otpl$origin <- c("myaccount_","myaccount","multipay_page")
  funnel_b2c_payment_otpl$to <- c("myaccount","multipay_page","multipay_finished")
  funnel_b2c_payment_otpl <- funnel_b2c_payment_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  funnel_trans_otpl <- funnel_trans_otpl[c(1,3,4,6)]
  colnames(funnel_trans_otpl) <- c("Values","Step","Overall Conversion","Step conversion")
  funnel_trans_otpl <- funnel_trans_otpl[,c("Step","Values","Step conversion","Overall Conversion")]
  funnel_trans_otpl$origin <- c("myaccount_","myaccount","multipay_page")
  funnel_trans_otpl$to <- c("myaccount","multipay_page","multipay_finished")
  funnel_trans_otpl <- funnel_trans_otpl[,c("origin","to","Values","Step conversion","Overall Conversion")]
  
  
  save(funnel_b2c_payment_otpl,file='funnel_b2c_payment_otpl.RData')
  save(funnel_trans_otpl,file='funnel_trans_otpl.RData')
  
  
  #Otodom Funnel List 
  
  
  
  # ------Transform Posting fORM--------------
  
  funnel_posting_otpl <- funnel_posting_otpl[c(1,2,3,4,11,12,13,14,21,22,23,24,31,32,33,34,41,42,43,44)]
  funnel_posting_otpl1 <- funnel_posting_otpl[c(1,2,3,4)]
  funnel_posting_otpl1$Week <- as.Date("2017-12-17")
  funnel_posting_otpl2 <- funnel_posting_otpl[c(5,6,7,8)]
  funnel_posting_otpl2$Week <- as.Date("2017-12-24")
  funnel_posting_otpl3 <- funnel_posting_otpl[c(9,10,11,12)]
  funnel_posting_otpl3$Week <- as.Date("2018-01-07")
  funnel_posting_otpl4 <- funnel_posting_otpl[c(13,14,15,16)]
  funnel_posting_otpl4$Week <- as.Date("2017-12-10")
  funnel_posting_otpl5 <- funnel_posting_otpl[c(17,18,19,20)]
  funnel_posting_otpl5$Week <- as.Date("2017-12-31")
  colnames(funnel_posting_otpl1) <- c("Values","Step conversion","Step","Overall Conversion","Week")
  colnames(funnel_posting_otpl2) <- c("Values","Step conversion","Step","Overall Conversion","Week")
  colnames(funnel_posting_otpl3) <- c("Values","Step conversion","Step","Overall Conversion","Week")
  colnames(funnel_posting_otpl4) <- c("Values","Step conversion","Step","Overall Conversion","Week")
  colnames(funnel_posting_otpl5) <- c("Values","Step conversion","Step","Overall Conversion","Week")
  funnel_posting_otpl <- rbind(funnel_posting_otpl1,funnel_posting_otpl2,funnel_posting_otpl3,funnel_posting_otpl4,funnel_posting_otpl5)
  funnel_posting_otpl$Step <- c("posting initial","select category","posting form","posting preview","posting ad")
  
  # funnel_posting_otpl$origin <- c("home","home","search_click","listingpage","adpage")
  # funnel_home_otpl$to <- c("home","search_click","listingpage","adpage","replies_home")
  # funnel_home_otpl <- funnel_home_otpl[,c("Week","origin","to","Values","Step conversion","Overall Conversion")]
  funnel_posting_otpl <- funnel_posting_otpl[order(as.Date(funnel_posting_otpl$Week)),]
  funnel_posting_otpl$Funnel <- c("Posting Form")
  funnel_posting_otpl$Platform <- c("Otodom PL")
  funnel_posting_otpl <- funnel_posting_otpl[,c("Week","Step","Values","Step conversion","Overall Conversion","Funnel","Platform")]
  
  
  save(funnel_posting_otpl,file="funnel_posting_otpl.RData")
  
  
  
  funnel_total_otpl_weeks <- rbind (funnel_home_otpl,funnel_list_otpl,funnel_adpage_otpl,funnel_posting_otpl)
  
  # funnel_total_otpl_per <- rbind (funnel_home_otpl,funnel_list_otpl,funnel_adpage_otpl)
  #funnel_total_otpl_per$"Overall Conversion" <- percent(funnel_total_otpl$"Overall Conversion")
  #funnel_total_otpl_per$"Step conversion" <- percent(funnel_total_otpl$"Step conversion")
  
  save(funnel_total_otpl_weeks,file="funnel_total_otpl_weeks.RData")
  #save(funnel_total_otpl_per,file="funnel_total_otpl_per.RData")
  
  
  --------------------------------------  --------------------------------------  --------------------------------------  --------------------------------------
    
    
    require(googleVis)
  
  plot(
    gvisSankey(funnel_trans_otpl, from="origin", 
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
  
  

  
  
  