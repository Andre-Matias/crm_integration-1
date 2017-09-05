Access DB configurations and credentials with R
================

Centralized files with DBs configuration and credentials. Using the **config** package you can:

- access DB configurations and credentials quickly within your R script;
- avoid to include DB credentials and password when pushing R code to Github.  

<br />

Getting Started
-----

Within the folder are included:

- a `config.yml`text file where all the DB configurations are defined (DbHost, DbPort, DbUser, DbName);
- a `credentials.Rdata` file which contains password for each of the DB defined in the `config.yml` file.


#### 1. Pull folder to your local repo 

Make sure to have an up-to-date version of the above files in your local machine.

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

#### 4. Import credentials (passwords) into your local environment

``` r
load("~/verticals-bi/yml_config/credentials.Rdata")
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

load("~/verticals-bi/yml_config/credentials.Rdata") 


# Example incorporating yml credentials in a DB connection to Stradia Argentina

library("RMySQL")

conDB<- dbConnect(MySQL(), 
                  user= config$DbUser, 
                  password= StradiaArDbPwd,  # comes with loading credentials.Rdata
                  host= config$DbHost, 
                  port= config$DbPort,
                  dbname = config$DbName
)

dbListTables(conDB)

dbDisconnect(conDB)
```

