### Template - Personal Credentials to access DB - Template ###################
#   *keep it private in your local machine*                                   #                                      
###############################################################################

# remove all object from workspace --------------------------------------------
rm(list = ls())


# OTOMOTO PL  BI Slave --------------------------------------------------------
OtomotoPlDbPwd <- ""


# STANDVIRTUAL PORTUGAL -------------------------------------------------------
StandvirtualPtDbPwd <- ""


# AUTOVITO RO -----------------------------------------------------------------
AutovitoRoDbPwd <- ""


# STRADIA INDIA ---------------------------------------------------------------
StradiaInDbPwd <- ""


#STRADIA ARGENTINA ------------------------------------------------------------
StradiaArDbPwd <- ""


#STRADIA COLOMBIA ------------------------------------------------------------
StradiaCoDbPwd <- ""


#STRADIA ECUADOR ------------------------------------------------------------
StradiaEcDbPwd <- ""


#STRADIA PERÚ ------------------------------------------------------------
StradiaPeDbPwd <- ""


# New Storia BI Slave (valid after code merge ) -------------------------------
StoriaIdDbPwd <- ""


# Silver Cluster in Amazon Redshift -------------------------------------------
SilverDwPwd <- ""

# save all object in workspace ------------------------------------------------
# ATTENTION save this file locally on your machine and make it chmod 400
# with this command chmod 440 ~/personal_credentials.RData
save(list = ls(), file= "~/personal_credentials.Rdata")
