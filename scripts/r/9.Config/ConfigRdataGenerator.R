# remove all object from workspace ---------------------------------------------------
rm(list = ls())

# Stradia India BI Slave -------------------------------------------------------------
cfStradiaInDbHost <- "192.168.1.5"
cfStradiaInDbPort <- "3312"
cfStradiaInDbUser <- "bi_team_pt"

# save all object in workspace -------------------------------------------------------
save(list = ls(), file= "GlobalConfig.Rdata")
