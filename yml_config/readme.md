Access DB configurations and credentials with R
================

Centralized files with DBs configuration and credentials template. Using the **config** package you can:

- access DB configurations and personal credentials quickly within your R script;
- avoid to include DB personal credentials when pushing R code to Github.  

<br />

Getting Started
-----

Within the folder are included:

- a `config.yml`text file where all the DB configurations are defined (DbHost, DbPort, DbUser, DbName);
- a `personal_credentials.R` file template where you can store all your personal credentials for each DB defined in the `config.yml` file.


#### 1. Pull folder into your local repo 


#### 2. Install **config** package in R

``` r
install.packages("config")
```

#### 3. Read configuration values

``` r
library("config")
```

Read a specific DB credentials from `config.yml`. For example for Otomoto Poland:

``` r
config <- config::get(file = "~/verticals-bi/yml_config/config.yml", 
                      config = Sys.getenv("R_CONFIG_ACTIVE", "otomoto_pl") )
```
To access any other DB just change the last argument, and use same nomenclature (e.g. "stradia_in", "stradia_ar"). Open `config.yml` file if you are not sure about the name.

Once the reading connection is established, you can access each credential of that DFB this way:

``` r
config$DbPort
config$DbHost
config$DbName   #press tab after the dollar sign to see which parameters are available
```


#### 4. Initiate `personal_credentials.R` with your own personal credentials

Open the `credentials.R template provided and fill it in with your own personal xredentials. Add any more you might need.

Run the script. 

Save the output as .Rdata as indicated in the template, so that you can import it later when needed.

IMPORTANT: Save it only in your local machine outside of vertical-bi repo. Do not pull it to github!


To import personal credentials into you R environment you'll just need to load it:

``` r
load("~/personal_credentials.Rdata")
```


<br />

Quick Example with R
---

```
library(config)

# Establish connection to yml config for Stradia Argentina

config <- config::get(file = "~/verticals-bi/yml_config/config.yml",
                      config = Sys.getenv("R_CONFIG_ACTIVE", "stradia_ar") )

# Load personal credentials

load("~/personal_credentials.Rdata") 


# Example incorporating yml credentials in a DB connection to Stradia Argentina

library("RMySQL")

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= StradiaArDbPwd,  # comes with loading personal_credentials.Rdata
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname = config$DbName
)

dbListTables(conDB)

dbDisconnect(conDB)
```

