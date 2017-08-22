# remove all object from workspace --------------------------------------------
rm(list = ls())


# Otomoto PL  BI Slave --------------------------------------------------------
cfOtomotoPLDbHost <- "192.168.1.5"
cfOtomotoPLDbPort <- "3317"
cfOtomotoPLDbUser <- "bi_team_pt"
cfOtomotoPLDbName <- "otomotopl"
cfOtomotoPLArchiveDbName <- "ads_history_otomotopl"

# Stradia India BI Slave -----------------------------------------------------
cfStradiaInDbHost <- "192.168.1.5"
cfStradiaInDbPort <- "3312"
cfStradiaInDbUser <- "bi_team_pt"
cfStradiaInDbName <- "cars_in"

# New Storia BI Slave (valid after code merge ) ------------------------------
cfStoriaIdDbHost <- "192.168.1.5"
cfStoriaIdDbPort <- "3315"
cfStoriaIdDbUser <- "bi_team_pt"



# save all object in workspace -----------------------------------------------
save(list = ls(), file= "~/GlobalConfig.Rdata")
