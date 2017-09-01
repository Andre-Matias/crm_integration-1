# remove all object from workspace ---------------------------------------------
rm(list = ls())


# Otomoto PL  BI Slave ---------------------------------------------------------
cfOtomotoPLDbHost <- "192.168.1.5"
cfOtomotoPLDbPort <- "3317"
cfOtomotoPLDbUser <- "bi_team_pt"
cfOtomotoPLDbName <- "otomotopl"
cfOtomotoPLArchiveDbName <- "ads_history_otomotopl"


# Stradia India BI Slave -------------------------------------------------------
cfStradiaInDbHost <- "192.168.1.5"
cfStradiaInDbPort <- "3312"
cfStradiaInDbUser <- "bi_team_pt"
cfStradiaInDbName <- "cars_in"


# Stradia Latam BI Slave (same for ar, co, ec, pe; just change the db name) ----
cfStradiaLatamDbHost <- "192.168.1.5"
cfStradiaLatamDbPort <- "3311"
cfStradiaLatamDbUser <- "bi_team_pt"

cfStradiaArDbName<- "stradia_ar"
cfStradiaCoDbName<- "stradia_co"
cfStradiaEcDbName<- "stradia_ec"
cfStradiaPeDbName<- "stradia_pe"


# New Storia BI Slave (valid after code merge ) --------------------------------
cfStoriaIdDbHost <- "192.168.1.5"
cfStoriaIdDbPort <- "3315"
cfStoriaIdDbUser <- "bi_team_pt"


# Standvirtual Portugal BI Slave -----------------------------------------------
cfStandvirtualPtDbHost <- "192.168.1.5"
cfStandvirtualPtDbPort <- "3308"
cfStandvirtualPtDbUser <- "bi_team_pt"
cfStandvirtualPtDbName <- "carspt"


# Autovito Romania BI Slave ---------------------------------------------------
cfAutovitoRoDbHost <- "192.168.1.5"
cfAutovitoRoDbPort <- "3316"
cfAutovitoRoDbUser <- "bi_team_pt"
cfAutovitoRoDbName <- "autovitro"


# Triton Silver ---------------------------------------------------------------
tritonDbHost <- 
  "olxgroup-bi-silver.cmljrugfoo4y.us-west-2.redshift.amazonaws.com"
tritonDbPort <- 5439
tritonDbName <- "olxgroupbi"



# save all object in workspace (no need anymore) ------------------------------
# save(list = ls(), file= "~/r_scripts_miei/GlobalConfig.Rdata")
