#####################################################################################################
#################################                                ####################################
#################################    WELCOME TO LUCYS' WORLD     ####################################
#################################                                ####################################
#####################################################################################################
#                  ::   : .'    ::-._                    `.:    `:            :   
#                  ::   `.:     ::   "-._                _.-----  :           :
#                  ::    `;     :  _.--.._""-.        _.-"..--._  :           :
#                  : :    `     '-"-"(("))\   `     .' /(("))"-"- :           :
#                  : :            `-.`-.-'_\   . .  . /_`-.-'.-'   :         :
#                  : `.              `"""'     : :  :   `"""'      :        :
#                  `. `.                       . .  .              :       ;
#################################################################  :      :
#################################################################  :     ;
#################################################################  :    :
#################################################################  :   :
###############################################################    :  :
##############################################################    ;  ;
#############################################################    ;  :
############################################################    :  '
###########################################################     :'




#########
## TRACKING MONITORING
#########
start.time <- Sys.time()
#setwd("~/Google Drive/Ninja Trackers/Lucy") # MAC
#setwd("C:/Users/Jeremy Castan/Google Drive/Ninja Trackers/Lucy") # PC
setwd("~/Lucy")

# loading configuration and libraries
source('config/main.R') # done
source('config/matrices.R') # done
source('config/yamato.R') # done
source('config/properties_classification.R') # done
source('config/query_builder_jql.R') # done
source('config/reference_tables.R') # done
source('config/apply_function.R') # done
source('config/functions.R') # done
source('config/audit_lists.R') # done



source('genesis/global_audit.R') # done





source('config/functionsb.R') # to do

save(verticals_list,file='verticals_list.RData')
load("verticals_list.RData")

source('genesis/cars/poland/desktop/ad_page.R')





















source('config/mixpanel_config.R')
source('hydra_config.R')
source('config/functionsb.R')

### load the tables

# test
('tests/ad_page_test.R')

#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ AUTOVIT RO

# desktop
source('cars/autovitro/desktop/ad_page.R') # cars_ro_d_ad_gDf
source('cars/autovitro/desktop/listing.R') # cars_ro_d_listing_gDf
source('cars/autovitro/desktop/posting_form.R') # cars_ro_d_posting_gDf
source('cars/autovitro/desktop/multipay.R') # cars_ro_d_multipay_gDf
source('cars/autovitro/desktop/seller_page.R') # cars_ro_d_seller_gDf
source('cars/autovitro/desktop/home.R') # cars_ro_d_home_gDf
source('cars/autovitro/desktop/my_account.R') # cars_ro_d_account_gDf
source('cars/autovitro/desktop/others.R') # cars_ro_d_others_gDf

# mobile web
source('cars/autovitro/mobile/ad_page.R') # cars_ro_m_ad_gDf
source('cars/autovitro/mobile/listing.R') # cars_ro_m_listing_gDf
source('cars/autovitro/mobile/posting_form.R') # cars_ro_m_posting_gDf
source('cars/autovitro/mobile/multipay.R') # cars_ro_m_multipay_gDf
source('cars/autovitro/mobile/seller_page.R') # cars_ro_m_seller_gDf
source('cars/autovitro/mobile/home.R') # cars_ro_m_home_gDf
source('cars/autovitro/mobile/my_account.R') # cars_ro_m_account_gDf
source('cars/autovitro/mobile/others.R') # cars_ro_m_others_gDf

#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ OTOMOTO PL

# desktop
source('cars/otomotopl/desktop/ad_page.R') # cars_pl_d_ad_gDf
source('cars/otomotopl/desktop/listing.R') # cars_pl_d_listing_gDf
source('cars/otomotopl/desktop/posting_form.R') # cars_pl_d_posting_gDf
source('cars/otomotopl/desktop/multipay.R') # cars_pl_d_multipay_gDf
source('cars/otomotopl/desktop/seller_page.R') # cars_pl_d_seller_gDf
source('cars/otomotopl/desktop/home.R') # cars_pl_d_home_gDf
source('cars/otomotopl/desktop/my_account.R') # cars_pl_d_account_gDf
source('cars/otomotopl/desktop/others.R') # cars_pl_d_others_gDf

# mobile web
source('cars/otomotopl/mobile/ad_page.R') # cars_pl_m_ad_gDf
source('cars/otomotopl/mobile/listing.R') # cars_pl_m_listing_gDf
source('cars/otomotopl/mobile/posting_form.R') # cars_pl_m_posting_gDf
source('cars/otomotopl/mobile/multipay.R') # cars_pl_m_multipay_gDf
source('cars/otomotopl/mobile/seller_page.R') # cars_pl_m_seller_gDf
source('cars/otomotopl/mobile/home.R') # cars_pl_m_home_gDf
source('cars/otomotopl/mobile/my_account.R') # cars_pl_m_account_gDf
source('cars/otomotopl/mobile/others.R') # cars_pl_d_others_gDf

#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ STANDVIRTUAL PT

# desktop
source('cars/standvirtualpt/desktop/ad_page.R') # cars_pt_d_ad_gDf
source('cars/standvirtualpt/desktop/listing.R') # cars_pt_d_listing_gDf
source('cars/standvirtualpt/desktop/posting_form.R') # cars_pt_d_posting_gDf
source('cars/standvirtualpt/desktop/multipay.R') # cars_pt_d_multipay_gDf
source('cars/standvirtualpt/desktop/seller_page.R') # cars_pt_d_seller_gDf
source('cars/standvirtualpt/desktop/home.R') # cars_pt_d_home_gDf
source('cars/standvirtualpt/desktop/my_account.R') # cars_pt_d_account_gDf
source('cars/standvirtualpt/desktop/others.R') # cars_pt_d_others_gDf

# mobile web
source('cars/standvirtualpt/mobile/ad_page.R') # cars_pt_m_ad_gDf
source('cars/standvirtualpt/mobile/listing.R') # cars_pt_m_listing_gDf
source('cars/standvirtualpt/mobile/posting_form.R') # cars_pt_m_posting_gDf
source('cars/standvirtualpt/mobile/multipay.R') # cars_pt_m_multipay_gDf
source('cars/standvirtualpt/mobile/seller_page.R') # cars_pt_m_seller_gDf
source('cars/standvirtualpt/mobile/home.R') # cars_pt_m_home_gDf
source('cars/standvirtualpt/mobile/my_account.R') # cars_pt_m_account_gDf
source('cars/standvirtualpt/mobile/others.R') # cars_pt_m_others_gDf

#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ IMOVIRTUAL PT

# desktop
source('real estate/imovirtualpt/desktop/ad_page.R') # re_pt_d_ad_gDf
source('real estate/imovirtualpt/desktop/listing.R') # re_pt_d_listing_gDf
source('real estate/imovirtualpt/desktop/posting_form.R') # re_pt_d_posting_gDf
source('real estate/imovirtualpt/desktop/multipay.R') # re_pt_d_multipay_gDf
source('real estate/imovirtualpt/desktop/seller_page.R') # re_pt_d_seller_gDf
source('real estate/imovirtualpt/desktop/home.R') # re_pt_d_home_gDf
source('real estate/imovirtualpt/desktop/my_account.R') # re_pt_d_account_gDf
source('real estate/imovirtualpt/desktop/others.R') # re_pt_d_others_gDf


# mobile web
source('real estate/imovirtualpt/mobile/ad_page.R') # re_pt_m_ad_gDf
source('real estate/imovirtualpt/mobile/listing.R') # re_pt_m_listing_gDf
source('real estate/imovirtualpt/mobile/posting_form.R') # re_pt_m_posting_gDf
source('real estate/imovirtualpt/mobile/multipay.R') # re_pt_m_multipay_gDf
source('real estate/imovirtualpt/mobile/seller_page.R') # re_pt_m_seller_gDf
source('real estate/imovirtualpt/mobile/home.R') # re_pt_m_home_gDf
source('real estate/imovirtualpt/mobile/my_account.R') # re_pt_m_account_gDf
source('real estate/imovirtualpt/mobile/others.R') # re_pt_m_others_gDf


#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ OTODOM PL

# desktop
source('real estate/otodompl/desktop/ad_page.R') # re_pl_d_ad_gDf
source('real estate/otodompl/desktop/listing.R') # re_pl_d_listing_gDf
source('real estate/otodompl/desktop/posting_form.R') # re_pl_d_posting_gDf
source('real estate/otodompl/desktop/multipay.R') # re_pl_d_multipay_gDf
source('real estate/otodompl/desktop/seller_page.R') # re_pl_d_seller_gDf
source('real estate/otodompl/desktop/home.R') # re_pl_d_home_gDf
source('real estate/otodompl/desktop/my_account.R') # re_pl_d_account_gDf
source('real estate/otodompl/desktop/others.R') # re_pl_d_others_gDf

# mobile web
source('real estate/otodompl/mobile/ad_page.R') # re_pl_m_ad_gDf
source('real estate/otodompl/mobile/listing.R') # re_pl_m_listing_gDf
source('real estate/otodompl/mobile/posting_form.R') # re_pl_m_posting_gDf
source('real estate/otodompl/mobile/multipay.R') # re_pl_m_multipay_gDf
source('real estate/otodompl/mobile/seller_page.R') # re_pl_m_seller_gDf
source('real estate/otodompl/mobile/home.R') # re_pl_m_home_gDf
source('real estate/otodompl/mobile/my_account.R') # re_pl_m_account_gDf
source('real estate/otodompl/mobile/others.R') # re_pl_m_others_gDf

#-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
#-~-~-~-~-~-~ STORIA RO

# desktop
source('real estate/storiaro/desktop/ad_page.R') # re_ro_d_ad_gDf
source('real estate/storiaro/desktop/listing.R') # re_ro_d_listing_gDf
source('real estate/storiaro/desktop/posting_form.R') # re_ro_d_posting_gDf
source('real estate/storiaro/desktop/multipay.R') # re_ro_d_multipay_gDf
source('real estate/storiaro/desktop/seller_page.R') # re_ro_d_seller_gDf
source('real estate/storiaro/desktop/home.R') # re_ro_d_home_gDf
source('real estate/storiaro/desktop/my_account.R') # re_ro_d_account_gDf
source('real estate/storiaro/desktop/others.R') # re_ro_d_others_gDf

# mobile web
source('real estate/storiaro/mobile/ad_page.R') # re_ro_m_ad_gDf
source('real estate/storiaro/mobile/listing.R') # re_ro_m_listing_gDf
source('real estate/storiaro/mobile/posting_form.R') # re_ro_m_posting_gDf
source('real estate/storiaro/mobile/multipay.R') # re_ro_m_multipay_gDf
source('real estate/storiaro/mobile/seller_page.R') # re_ro_m_seller_gDf
source('real estate/storiaro/mobile/home.R') # re_ro_m_home_gDf
source('real estate/storiaro/mobile/my_account.R') # re_ro_m_account_gDf
source('real estate/storiaro/mobile/others.R') # re_ro_m_others_gDf

# check the time 
end.time <- Sys.time()
time.taken <- end.time - start.time
print(paste('Audit took',round(time.taken,2),'min'))

# remove variables
rm(list=setdiff(ls(), ls()[c(grep('_gDf',ls()))]))

# events quality
source('config/quality_processing.R')












## IN DEVELOPMENT
#########



# sheets
cars <- gs_key('1zFDilfbenmlaUIBPpm0FlUZy0p-Xvpbp-EOHeFc1MfI')
realEstate <- gs_key('1NckjEISsCDsjvCJB6ec-g6Jd5PS0OgWsxy37aQdj3O0')

# 1- import all the final tables there
# 2- rbind them
# 3- import in report
# 4- import in detailed report

### update google sheets
# columns to select
columns <- c('vertical','platform','universe','name','event_quality')

### CARS
gs_edit_cells(
  cars,
  ws = 'report',
  input = otomoto_desktop_adpage[columns],
  anchor = 'A1')