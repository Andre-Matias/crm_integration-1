# remove all object from workspace ---------------------------------------------------
rm(list = ls())


# Stradia India BI Slave -------------------------------------------------------------
cfOtomotoPLDbHost <- "192.168.1.5"
cfOtomotoPLDbPort <- "3317"
cfOtomotoPLDbUser <- "bi_team_pt"
cfOtomotoPLDbName <- "otomotopl"
cfOtomotoPLArchiveDbName <- "ads_history_otomotopl"


# Stradia India BI Slave -------------------------------------------------------------
cfOtomotoPLInDbHost <- "192.168.1.5"
cfOtomotoPLDbPort <- "3317"
cfOtomotoPLDbUser <- "bi_team_pt"

# Stradia India BI Slave -------------------------------------------------------------
cfStradiaInDbHost <- "192.168.1.5"
cfStradiaInDbPort <- "3312"
cfStradiaInDbUser <- "bi_team_pt"

# New Storia BI Slave (valid after code merge ) --------------------------------------
cfStoriaInDbHost <- "192.168.1.5"
cfStradiaInDbPort <- "3315"
cfStradiaInDbUser <- "bi_team_pt"

# save all object in workspace -------------------------------------------------------
save(list = ls(), file= "~/GlobalConfig.Rdata")
