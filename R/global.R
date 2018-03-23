# load libraries --------------------------------------------------------------
library("aws.s3")

# load mixpanel user's credentials --------------------------------------------
load("~/GlobalConfig.Rdata")
load("~/credentials.Rdata")

Sys.setenv("AWS_ACCESS_KEY_ID" = myS3key,
           "AWS_SECRET_ACCESS_KEY" = MyS3SecretAccessKey)

CARS5999_df <- 
  reactive({
  invalidateLater(1000, session)
  dfAll <- 
    s3readRDS(object = "CARS-5999/dfDAU_AutovitRO.RDS", 
              bucket = "pyrates-data-ocean/GVPI-112"
    )
})