# credentials
pw <- {'dfgksjkDSGZKAjk35235SFAFkjkj'}
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv,dbname='main',host='10.101.5.159'
                 ,port=5671,user='jeremy_castan',password=pw)
rm(pw)

# Yamato categories

autovit_cat1 <- dbGetQuery(con,
                           "select id as l1_id,name_en as l1_name,code
                           from db_atlas_verticals.categories
                           where filter_label_en = 'Category'
                           and livesync_dbname = 'autovitro'")
autovit_cat1$l1_name <- tolower(autovit_cat1$l1_name)

autovit_cat2 <- dbGetQuery(con,
                           "select id as l2_id, name_en as l2_name, parent_id as l1_id
                           from db_atlas_verticals.categories
                           where filter_label_en = 'Subcategory'
                           and livesync_dbname = 'autovitro'")
autovit_cat2$l2_name <- tolower(autovit_cat2$l2_name)

otomoto_cat1 <- dbGetQuery(con,
                           "select id as l1_id,name_en as l1_name,code
                           from db_atlas_verticals.categories
                           where filter_label_en = 'Category'
                           and livesync_dbname = 'otomotopl'")
otomoto_cat1$l1_name <- tolower(otomoto_cat1$l1_name)

otomoto_cat2 <- dbGetQuery(con,
                           "select id as l2_id, name_en as l2_name, parent_id as l1_id
                           from db_atlas_verticals.categories
                           where filter_label_en = 'Subcategory'
                           and livesync_dbname = 'otomotopl'")
otomoto_cat2$l2_name <- tolower(otomoto_cat2$l2_name)

standvirtual_cat1 <- dbGetQuery(con,
                                "select id as l1_id,name_en as l1_name,code
                                from db_atlas_verticals.categories
                                where livesync_dbname = 'carspt'")
standvirtual_cat1$l1_name <- tolower(standvirtual_cat1$l1_name)

imovirtual_cat1 <- dbGetQuery(con,
                              "select id as l1_id,name_en as l1_name,code,parent_id
                              from db_atlas_verticals.categories
                              where livesync_dbname = 'imovirtualpt'
                              and parent_id != 0")

otodompl_cat1 <- dbGetQuery(con,
                            "select id as l1_id,name_en as l1_name,code,parent_id
                            from db_atlas_verticals.categories
                            where livesync_dbname = 'otodompl'
                            and parent_id != 0")

storiaro_cat1 <- dbGetQuery(con,
                            "select id as l1_id,name_en as l1_name,code,parent_id
                            from db_atlas_verticals.categories
                            where livesync_dbname = 'storiaro'
                            and parent_id != 0")

# Yamato locations
autovit_regions <- dbGetQuery(con,
                              "select id as region_id,name_en as region_name
                              from db_atlas_verticals.regions
                              where livesync_dbname = 'autovitro'")
autovit_cities <- dbGetQuery(con,
                             "select id as city_id,name_en as city_name
                             from db_atlas_verticals.cities
                             where livesync_dbname = 'autovitro'")

otomoto_regions <- dbGetQuery(con,
                              "select id as region_id,name_en as region_name
                              from db_atlas_verticals.regions
                              where livesync_dbname = 'otomotopl'")

otomoto_cities <- dbGetQuery(con,
                             "select id as city_id,name_en as city_name
                             from db_atlas_verticals.cities
                             where livesync_dbname = 'otomotopl'")

standvirtual_regions <- dbGetQuery(con,
                                   "select id as region_id,name_en as region_name
                                   from db_atlas_verticals.regions
                                   where livesync_dbname = 'carspt'")

standvirtual_cities <- dbGetQuery(con,
                                  "select id as city_id,name_en as city_name
                                  from db_atlas_verticals.cities
                                  where livesync_dbname = 'carspt'")

imovirtual_regions <- dbGetQuery(con,
                                 "select id as region_id,name_en as region_name
                                   from db_atlas_verticals.regions
                                   where livesync_dbname = 'imovirtualpt'")

imovirtual_cities <- dbGetQuery(con,
                                "select id as city_id,name_en as city_name
                                  from db_atlas_verticals.cities
                                  where livesync_dbname = 'imovirtualpt'")

otodompl_regions <- dbGetQuery(con,
                               "select id as region_id,name_en as region_name
                                 from db_atlas_verticals.regions
                                 where livesync_dbname = 'otodompl'")

otodompl_cities <- dbGetQuery(con,
                              "select id as city_id,name_en as city_name
                                from db_atlas_verticals.cities
                                where livesync_dbname = 'otodompl'")

storiaro_regions <- dbGetQuery(con,
                               "select id as region_id,name_en as region_name
                               from db_atlas_verticals.regions
                               where livesync_dbname = 'storiaro'")

storiaro_cities <- dbGetQuery(con,
                              "select id as city_id,name_en as city_name
                              from db_atlas_verticals.cities
                              where livesync_dbname = 'storiaro'")

dbDisconnect(con)
