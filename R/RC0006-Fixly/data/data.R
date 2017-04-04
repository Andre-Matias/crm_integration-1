library("reshape2")
library("reshape")
#load Data Table
library("dplyr")
 
library("RPostgreSQL")
 
drv <- dbDriver("PostgreSQL")

conn_chandra <- dbConnect(drv, host="gv-chandra.ckwrimb1igb1.us-west-2.redshift.amazonaws.com", 
                    port="5439",
                    dbname="globalverticals", 
                    user="pyrate", 
                    password="Pyrate4life")

# conn_dw <- dbConnect(drv, host="bi-public-warehouse.onap.io", 
#                      port="5439",
#                      dbname="naspersdw", 
#                      user="danielrocha", 
#                      password="")
 
res <- dbSendQuery(conn_chandra, "select bucket,
                              count(distinct user_id)
                              from public.fixly_buckets
                              group by bucket")

totalUsersPerBucket <-dbFetch(res)

totalUsersPerBucket$Pct <- round((totalUsersPerBucket$count / sum(totalUsersPerBucket$count))*100,0)
totalUsersPerBucket$label <- paste0(totalUsersPerBucket$bucket, ", ", totalUsersPerBucket$Pct, "%")


df_desc <- data.frame('A'=c("Gold","Silver","Bronze","Tin","Drop-off"),
                      'B'=c(8715,8552,30749,227480,135086),
                      'C'=c(2,2,7,55,33),
                      'D'=c(52668,8552,64002,564961,0),
                      'E'=c(8,1,9,82,0),
                      'F'=c(532012.26,274995.89,20211.64,0,0),
                      'G'=c(64,33,2,0,0),
                      'H'=c("More than 1 active ad and at least 1 VAS purchase within last month",
                                     "1 active ad and at least 1 VAS purchase within last month",
                                     "At least 1 active ad, 0 payments within last month, at least 1 payment within last 6 month + 0 active ads and at least 1 payment within last 6 months",
                                     "At least 1 active ad and 0 payments within last 6 month",
                                     "Zero ads active, zero payments within last 6 months, and at least 1 ad added within last 6 months"),
                       stringsAsFactors=FALSE)


df_teste <- data.frame(Service=c("Budowa i Remont",
                                 "Muzycy",
                                 "Obsuga imprez",
                                 "Pozostae usugi",
                                 "Serwis RTV i AGD",
                                 "Sprztanie",
                                 "Tumaczenia",
                                 "Usugi finansowe",
                                 "Usugi informatyczne",
                                 "Usugi kosmetyczne",
                                 "Usugi motoryzacyjne",
                                 "Usugi ogrodnicze",
                                 "Usugi reklamowe",
                                 "Usugi transportowe",
                                 "Usugi zdrowotne",
                                 "Wspraca biznesowa",
                                 "Wyposaenie firm"),
                       CATEGORYID=c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17),
                       CATEGORY=c("Construction and Renovation",
                                  "Musicians",
                                  "Party service",
                                  "other services",
                                  "Home Appliances",
                                  "Cleaning",
                                  "Translations",
                                  "Financial services",
                                  "IT services",
                                  "Cosmetic services",
                                  "Automotive services",
                                  "Gardening services",
                                  "Advertising services",
                                  "Transportation services",
                                  "Health services",
                                  "Business cooperation",
                                  "equipment companies"),
                       GOLDBOTH=c(738,27,140,618,68,156,9,73,58,211,436,175,20,114,14,177,728),
                       SILVERBOTH=c(705,24,32,341,17,93,4,72,15,97,190,101,24,105,58,53,262),
                       BRONZEBOTH=c(1768,123,111,413,106,259,44,236,176,223,201,208,133,504,253,798,819),
                       GOLD=c(2459,53,280,1546,135,521,29,364,289,421,872,584,197,1143,144,442,1821),
                       SILVER=c(1409,59,161,682,43,186,44,241,152,485,380,253,81,525,192,264,1311),
                       BRONZE=c(5893,245,1105,4127,212,1297,148,1180,882,2227,2011,692,663,2519,843,1596,8193),
                       stringsAsFactors=FALSE)


df_teste$TOTALBOTH <- df_teste$GOLDBOTH+df_teste$SILVERBOTH+df_teste$BRONZEBOTH
df_teste$TOTAL <- df_teste$GOLD+df_teste$SILVER+df_teste$BRONZE


df_unpvot <- melt(df_teste, id.vars = c("Service","CATEGORY","CATEGORYID"))

datn <- read.table(header=TRUE, text='
supp dose length
  OJ  0.5  13.23
  OJ  1.0  22.70
  OJ  2.0  26.06
  VC  0.5   7.98
  VC  1.0  16.77
  VC  2.0  26.14
')



df_teste_daily <- data.frame(CATEGORY=c('Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Construction and Renovation','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Musicians','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','Party service','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','other services','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Home Appliances','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Cleaning','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Translations','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','Financial services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','IT services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Cosmetic services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Automotive services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Gardening services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Advertising services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Transportation services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Health services','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','Business cooperation','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies','equipment companies'),
                            CATEGORYID=c(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,12,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,13,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,15,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17),
                            DIA=c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30),
                            GOLD=c(82,82,83,77,75,78,77,75,85,84,79,84,75,84,75,77,80,83,78,84,76,83,83,85,78,85,82,75,79,80,2,2,1,1,1,2,1,1,1,1,1,2,2,2,2,2,2,1,2,1,2,2,1,1,2,1,2,1,2,2,10,11,8,9,11,12,7,6,8,9,11,12,6,7,7,9,12,11,6,11,12,10,10,11,10,6,7,8,11,50,51,51,53,54,52,52,49,51,48,49,54,50,52,48,48,50,54,51,53,50,52,50,51,50,49,54,51,53,54,49,5,2,4,2,7,8,2,1,2,8,5,2,1,1,2,2,8,2,1,5,2,2,7,4,2,4,5,2,4,8,21,15,18,21,21,17,19,15,16,17,19,19,17,20,15,16,16,21,19,17,21,21,14,16,14,21,15,21,20,15,0,3,3,2,0,0,0,0,0,0,0,0,0,4,0,0,1,2,1,1,2,1,4,0,0,2,0,0,4,3,16,11,11,14,15,13,12,14,11,14,9,13,14,13,12,16,13,11,9,16,15,13,12,15,15,9,10,12,10,14,11,9,9,13,6,7,13,10,10,13,10,8,11,13,13,7,12,8,13,12,11,10,8,12,11,12,6,9,9,13,13,18,18,13,18,13,16,17,18,15,14,14,16,18,18,13,12,15,17,15,11,13,18,18,12,13,14,15,16,13,32,30,32,33,33,30,27,30,27,31,32,30,28,32,29,29,28,32,31,26,29,32,26,28,28,31,31,33,32,27,23,19,19,22,22,23,19,16,20,22,20,21,16,21,20,23,19,22,19,16,23,22,23,23,21,23,17,19,21,21,6,3,3,8,10,4,4,8,9,10,10,6,9,4,10,3,5,9,3,7,9,3,3,10,3,4,3,6,9,3,42,35,38,38,35,41,41,37,38,39,39,35,40,41,41,36,37,35,39,42,41,38,41,40,38,42,36,37,39,41,4,3,6,8,5,2,5,4,7,7,3,1,4,8,1,8,4,3,6,6,7,8,8,5,5,4,1,6,2,5,11,12,13,18,17,16,18,17,15,16,15,13,17,17,16,11,13,11,15,14,11,12,12,15,18,13,11,12,16,18,57,63,59,63,64,57,63,58,63,61,58,62,64,61,59,61,62,60,58,64,62,61,63,62,63,60,64,57,58,61),
                            SILVER=c(45,47,44,46,41,44,45,47,48,42,44,44,45,44,45,41,43,47,47,43,47,41,43,42,44,45,44,42,43,42,1,1,1,1,1,1,2,1,1,1,2,2,2,1,1,1,2,1,1,1,1,2,1,1,1,2,1,2,2,1,7,3,6,6,4,3,5,6,5,4,6,4,7,3,7,4,6,4,3,3,4,6,6,5,6,5,5,7,6,21,25,25,19,19,23,22,23,19,23,25,19,23,21,23,20,21,22,24,24,24,23,23,22,25,23,23,19,23,23,24,0,0,3,5,4,0,3,4,3,0,3,4,2,0,4,3,0,0,1,0,0,2,4,1,2,0,3,5,0,4,8,6,4,8,5,9,4,4,4,3,9,10,4,4,4,9,4,10,8,9,8,8,6,5,3,5,8,6,6,10,0,0,3,1,1,1,1,2,5,4,4,0,1,1,4,3,3,0,0,0,4,0,3,1,3,2,0,0,0,0,9,12,12,10,7,10,8,11,5,5,7,7,5,10,9,12,8,7,6,9,9,10,11,11,7,6,12,9,9,12,4,2,6,2,4,7,6,9,6,9,5,4,5,7,9,2,7,2,3,8,6,8,3,4,3,3,9,3,3,9,16,15,18,19,14,14,13,13,15,16,20,14,13,17,17,14,18,16,17,14,18,19,14,18,15,18,19,18,17,19,14,14,10,15,14,11,15,15,10,10,12,11,12,13,13,10,10,12,9,16,15,11,13,12,16,11,15,10,10,9,10,7,11,11,10,7,6,7,12,12,12,11,10,11,6,11,5,10,9,11,6,11,10,11,7,7,9,11,10,5,5,3,0,3,6,5,3,4,5,0,4,0,3,0,1,3,5,4,0,0,0,3,2,6,1,6,6,2,0,2,18,18,21,21,20,18,19,17,16,20,21,20,16,16,16,18,15,17,19,19,17,15,14,14,15,14,16,18,18,20,4,10,3,7,7,4,9,5,9,3,6,10,9,4,9,7,10,6,9,3,6,4,9,8,9,9,5,4,9,9,5,12,8,12,6,10,11,6,9,9,9,6,9,9,6,10,5,6,6,7,9,12,8,6,11,10,10,12,9,8,43,42,43,45,46,43,43,41,43,43,45,42,42,45,41,44,44,41,45,47,46,45,47,44,43,44,42,45,41,43),
                            BRONZE=c(197,193,192,197,196,194,192,195,198,194,196,197,193,192,191,191,196,195,194,194,191,191,196,196,198,196,197,198,191,192,7,10,8,10,6,8,10,8,10,7,8,6,8,8,8,6,6,10,9,6,9,6,7,9,7,8,6,9,6,9,35,37,37,36,35,37,38,37,34,39,37,37,37,39,34,33,36,34,35,37,33,36,35,35,35,38,36,36,35,140,140,139,139,135,135,136,138,136,133,135,139,137,135,139,133,136,139,140,133,139,136,135,138,135,140,136,135,138,135,140,9,6,7,8,6,11,6,9,6,11,7,7,11,9,5,10,9,8,6,8,4,9,5,10,4,10,11,7,8,10,46,46,45,42,45,44,41,40,42,43,41,44,43,47,44,45,41,47,41,40,47,46,41,47,44,45,47,43,47,44,8,5,7,2,2,5,7,6,6,6,1,6,2,6,4,1,8,5,8,8,1,8,3,4,4,8,6,6,6,6,42,41,38,39,37,42,40,38,37,41,36,41,38,40,39,38,42,39,37,38,36,37,43,39,38,42,37,38,36,42,28,27,26,28,33,33,29,30,29,28,33,30,32,28,26,33,33,27,26,29,27,31,32,32,31,26,29,31,31,33,74,74,71,72,72,73,75,74,77,77,72,74,77,71,71,73,75,71,72,78,78,72,73,78,71,78,74,73,75,78,66,68,65,67,67,71,71,66,64,71,70,71,64,67,65,67,71,71,68,68,65,68,64,69,66,67,64,66,71,67,23,23,25,25,23,20,22,21,24,22,22,26,21,26,25,27,25,20,22,27,20,23,21,24,27,26,27,20,22,20,26,23,21,25,19,19,19,19,25,22,21,26,24,21,20,21,23,21,19,26,21,20,25,20,22,24,20,19,24,25,80,83,86,81,85,87,81,80,85,84,86,83,82,87,83,80,84,81,81,83,82,83,86,84,87,84,81,87,84,86,28,30,32,32,28,27,26,29,28,30,29,28,26,26,27,30,29,25,28,31,25,25,27,27,30,29,29,26,26,27,52,50,53,51,54,50,57,57,57,53,57,51,53,51,55,51,57,51,56,50,55,56,50,57,56,54,50,54,54,55,274,274,272,270,274,270,273,274,271,271,270,276,274,270,271,271,271,270,271,271,275,270,273,271,272,275,273,270,270,272),
                            stringsAsFactors=FALSE)



df_unpvot_daily <- melt(df_teste_daily, id.vars = c("CATEGORY","CATEGORYID","DIA"))
DT<-NULL
DT$CATEGOTY <- "TOTAL"
#####Add Total to DataStandVirtual#####
DT <- as.data.frame(df_teste  %>%  summarise(GOLDBOTH = sum(GOLDBOTH), 
                                            SILVERBOTH = sum(SILVERBOTH), 
                                            BRONZEBOTH = sum(BRONZEBOTH),
                                            GOLD = sum(GOLD),
                                            SILVER = sum(SILVER),
                                            BRONZE = sum(BRONZE)))





dbClearResult(dbListResults(conn_chandra)[[1]])

dbDisconnect(conn_chandra)

# melt(df_teste_daily, id.vars = c("CATEGORY","CATEGORYID","DIA"))

#saveRDS(df_teste_daily,"df_teste_daily.RDS")
