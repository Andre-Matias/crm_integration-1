library("reshape2")

library("RPostgreSQL")

drv <- dbDriver("PostgreSQL")

conn_chandra <- dbConnect(drv, host="gv-chandra.ckwrimb1igb1.us-west-2.redshift.amazonaws.com",
                     port="5439",
                     dbname="globalverticals",
                     user="pyrate",
                     password="Pyrate4life")

res <- dbSendQuery(conn_chandra, "select
                        name_en as Service,
                        new_id as CATEGORYID,
                        name_en as CATEGORY,
                        sum(case when BUCKET='GOLD' then 1 else 0 end)  as \"gold olx\",
                        20 as \"gold olx and fixly\",
                        sum(case when BUCKET='SILVER' then 1 else 0 end) as \"silver olx\",
                        0 as \"silver olx and fixly\",
                        sum(case when BUCKET='BRONZE' then 1 else 0 end) \"bronze olx\",
                        500 as \"bronze olx and fixly\",
                        sum(1) \"total olx\",
                        500 as \"total olx and fixly\"
                    from odl_global_verticals.vert_services_fixly_buckets_segment b
                      join md_category_english c
                        on b.category_id=c.id
                    where email is not null
                    group by c.new_id, name_en;
                   ")
df_teste <-dbFetch(res)
df_teste <- data.frame(df_teste, stringsAsFactors=FALSE)
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "select
                                    user_id as \"User Id\",
                                    email as \"E-mail\",
                                    phone as \"Phone\",
                                    c.new_id as categoryid,
                                    name_en as \"Category\",
                                    sum(nnls) as \"NNL 6 Months\",
                                    sum(active_ads) as \"Active Ads\",
                                    sum(reve1m) as \"Revenue 1 Month\",
                                    sum(reve1m) as \"Revenue 6 Months\",
                                    b.bucket,
                                    'FALSE' as Onboarded
                                  from odl_global_verticals.vert_services_fixly_buckets_segment b
                                    join md_category_english c
                                      on b.category_id=c.id
                                  where email is not null
                                  group by  user_id, email, phone, c.new_id, name_en, b.bucket")
df_prof_bucket <-dbFetch(res)
df_prof_bucket <- data.frame(df_prof_bucket, stringsAsFactors=FALSE)
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "
                                with registration as (
                                   select
                                   c.new_id categoryid,
                                   c.name_en category,
                                   b.bucket,
                                   trunc(created_at) created_at,
                                   count(distinct u.email) n_reg
                                   from odl_global_verticals.vert_services_fixly_buckets_segment b
                                   join public.md_category_english c
                                   on b.category_id=c.id
                                   left join rdl_vertical_services.fixpl_users u
                                   on b.email=u.email
                                   or (b.category_id=131 and b.bucket='GOLD')
                                   group by  b.bucket, c.new_id, c.name_en,trunc(created_at))
                                   select bucket, categoryid, category, isnull(created_at, trunc(getdate())) created_at, n_reg, sum(n_reg) over (partition by bucket, category, categoryid order by created_at rows unbounded preceding)
                                   from registration
                                   order by bucket, categoryid")

df_teste_daily <- dbFetch(res)
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "select bucket,
                             count(distinct user_id)
                             from odl_global_verticals.vert_services_fixly_buckets_segment
                             group by bucket")

totalUsersPerBucket <-dbFetch(res)

totalUsersPerBucket$Pct <- round((totalUsersPerBucket$count / sum(totalUsersPerBucket$count))*100,0)
totalUsersPerBucket$label <- paste0(totalUsersPerBucket$bucket, ", ", totalUsersPerBucket$Pct, "%")
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "
                                with users as(
                                  select count(DISTINCT user_id) total_users from odl_global_verticals.vert_services_fixly_buckets_segment
                                ),
                                buckets as (
                                    SELECT
                                      bucket,
                                      user_id,
                                      active_ads,
                                      reve1m,
                                      sum(active_ads)
                                      OVER () AS total_active_ads,
                                      sum(reve1m)
                                      OVER () AS total_vas,
                                      max(inserted_date) OVER () as inserted_date
                                    FROM odl_global_verticals.vert_services_fixly_buckets_segment
                                    where email is not null
                                )
                                 select
                                   bucket,
                                   count(DISTINCT user_id) as \"# professionals\",
                                   convert(varchar,round(count(DISTINCT user_id)*100.0 /(total_users),0))+ '% ' as \"% professionals\",
                                   sum(active_ads) as \"# active ads\",
                                   convert(varchar,round(sum(active_ads)*100.0/max(total_active_ads),0)) + '% ' as \"% active ads\",
                                   sum(reve1m) as \"VAS generated revenue\",
                                   convert(varchar,round(sum(reve1m)*100.0/max(total_vas),0)) + '% ' as \"%VAS generated revenue\",
                                   inserted_date
                                 from buckets, users
                                 group by bucket, total_users,inserted_date
                                order by 6 desc, 4 desc")

df_desc <- dbFetch(res)
dbClearResult(dbListResults(conn_chandra)[[1]])

df_unpvot <- melt(df_teste, id.vars = c("service","category","categoryid"))

df_desc$Definition <- c("More than 1 active ad and at least 1 VAS purchase within last month",
                         "1 active ad and at least 1 VAS purchase within last month",
                         "At least 1 active ad, 0 payments within last month, at least 1 payment within last 6 month + 0 active ads and at least 1 payment within last 6 months",
                         "At least 1 active ad and 0 payments within last 6 month",
                         "Zero ads active, zero payments within last 6 months, and at least 1 ad added within last 6 months")


### data from trackers ###
  # getting the tables
df_mau <- dbSendQuery(conn_chandra,
                        "SELECT *
                        FROM rdl_vertical_services.trackers_monthly_traffic
                        WHERE tracker = 'ga'")
df_mau <- dbFetch(df_mau)
df_mau$month <- 
dbClearResult(dbListResults(conn_chandra)[[1]])

df_traffic <- dbSendQuery(conn_chandra,
                          "SELECT *
                          FROM rdl_vertical_services.trackers_general_traffic
                          WHERE tracker = 'ga'
                          ORDER BY date ASC")
df_traffic <- dbFetch(df_traffic)
df_traffic$yearmonth <- substr(gsub('-','',df_traffic$date),0,6)
df_traffic$bounce_rate <- round((df_traffic$bounces / df_traffic$sessions) *100,2)
df_traffic <- merge(df_traffic,df_mau, by='yearmonth')
df_traffic <- rename(df_traffic, c('users.x'='dau','users.y'='mau'))
df_traffic$stickiness <- round((df_traffic$dau / df_traffic$mau)*100,2)

dbClearResult(dbListResults(conn_chandra)[[1]])

df_monthlyDB <- dbSendQuery(conn_chandra,
                            "with sums as (
                            SELECT
                            to_char(date_posted, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) active_users,
                            count(*) as nb_requests,
                            avg(rating_professional) average_pro_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            satisfied_req as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) satisfied_users,
                            count(*) satisfied_requests
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE date_satisfied IS NOT NULL
                            GROUP BY monthyear
                            ),
                            answ_users as (
                            SELECT
                            to_char(date_first_reply, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) answered_users
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE qty_answers > 0
                            GROUP BY monthyear
                            ),
                            u_avg_rating as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            avg(rating_professional) avg_user_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            p_avg_rating as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            avg(rating_user) avg_pro_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            p_raters1 as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) p_raters_1star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_user = 1
                            GROUP BY monthyear
                            ),
                            p_raters2 as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) p_raters_2stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_user = 2
                            GROUP BY monthyear
                            ),
                            p_raters3 as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) p_raters_3stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_user = 3
                            GROUP BY monthyear
                            ),
                            p_raters4 as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) p_raters_4stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_user = 4
                            GROUP BY monthyear
                            ),
                            p_raters5 as (
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) p_raters_5stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_user = 5
                            GROUP BY monthyear
                            ),
                            u_raters1 as(
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) u_raters_1star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_professional = 1
                            GROUP BY monthyear
                            ),
                            u_raters2 as(
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) u_raters_2star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_professional = 2
                            GROUP BY monthyear
                            ),
                            u_raters3 as(
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) u_raters_3star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_professional = 3
                            GROUP BY monthyear
                            ),
                            u_raters4 as(
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) u_raters_4star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_professional = 4
                            GROUP BY monthyear
                            ),
                            u_raters5 as(
                            SELECT
                            to_char(date_satisfied, 'YYYY-MM') monthyear,
                            count(DISTINCT user_id) u_raters_5star
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE rating_professional = 5
                            GROUP BY monthyear
                            )
                            SELECT *
                            FROM sums a
                            JOIN satisfied_req b ON a.monthyear = b.monthyear
                            JOIN answ_users c ON a.monthyear = c.monthyear
                            JOIN u_avg_rating d ON a.monthyear = d.monthyear
                            JOIN p_avg_rating e ON a.monthyear = e.monthyear
                            JOIN p_raters1 pr1 ON a.monthyear = pr1.monthyear
                            JOIN p_raters2 pr2 ON a.monthyear = pr2.monthyear
                            JOIN p_raters3 pr3 ON a.monthyear = pr3.monthyear
                            JOIN p_raters4 pr4 ON a.monthyear = pr4.monthyear
                            JOIN p_raters5 pr5 ON a.monthyear = pr5.monthyear
                            JOIN u_raters1 ur1 ON a.monthyear = ur1.monthyear
                            JOIN u_raters2 ur2 ON a.monthyear = ur2.monthyear
                            JOIN u_raters3 ur3 ON a.monthyear = ur3.monthyear
                            JOIN u_raters4 ur4 ON a.monthyear = ur4.monthyear
                            JOIN u_raters5 ur5 ON a.monthyear = ur5.monthyear")
df_monthlyDB <- dbFetch(df_monthlyDB)

df_globalDB <- dbSendQuery(conn_chandra,
                           "with req_table as (
                           SELECT
                           count(*) total_requests,
                           avg(rating_professional) u_average_rating,
                           avg(rating_user) p_average_rating
                           FROM odl_global_verticals.vert_services_fixly_requests_l3_city),
                           pro_table as (
                           SELECT
                           sum(qty_quotes) quotes,
                           sum(qty_approved_quotes) approved_quotes
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city),
                           active_pros as (
                           SELECT
                           count(*) active_pros
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_quotes > 0
                           ),
                           approved_pros as (
                           SELECT
                           count(*) approved_pros
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_approved_quotes > 0
                           ),
                           user_table as(
                           SELECT
                           sum(qty_answered_requests) answered_requests,
                           sum(qty_satisfied_requests) satisfied_requests,
                           count(*) active_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city),
                           answered as (
                           SELECT
                           count(*) answered_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_answered_requests > 0
                           ),
                           satisfied as (
                           SELECT
                           count(*) satisfied_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_satisfied_requests > 0
                           ),
                           u_raters1 as (
                           SELECT
                           count(*) u_raters_1star
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_1star_rated > 0
                           ),
                           u_raters2 as (
                           SELECT
                           count(*) u_raters_2stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_2star_rated > 0
                           ),
                           u_raters3 as (
                           SELECT
                           count(*) u_raters_3stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_3star_rated > 0
                           ),
                           u_raters4 as (
                           SELECT
                           count(*) u_raters_4stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_4star_rated > 0
                           ),
                           u_raters5 as (
                           SELECT
                           count(*) u_raters_5stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_5star_rated > 0
                           ),
                           p_raters1 as (
                           SELECT
                           count(*) p_raters_1star
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_1star > 0
                           ),
                           p_raters2 as (
                           SELECT
                           count(*) p_raters_2stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_2star > 0
                           ),
                           p_raters3 as (
                           SELECT
                           count(*) p_raters_3stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_3star > 0
                           ),
                           p_raters4 as (
                           SELECT
                           count(*) p_raters_4stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_4star > 0
                           ),
                           p_raters5 as (
                           SELECT
                           count(*) p_raters_5stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_5star > 0
                           )
                           SELECT *
                           FROM
                           req_table,pro_table,user_table,answered,satisfied,active_pros,approved_pros,
                           u_raters1,u_raters2,u_raters3,u_raters4,u_raters5,
                           p_raters1,p_raters2,p_raters3,p_raters4,p_raters5")

df_globalDB <- dbFetch(df_globalDB)


  # getting the boxes
month <- substr(gsub('-','', as.character(Sys.Date()-1)),0,6)


box_mau <- df_mau$users[df_mau$yearmonth == month]
box_bounce_rate <- paste0(round(sum(df_traffic$bounces) / sum(df_traffic$sessions)*100,2),'%')
box_stickiness <- paste0(round(mean((df_traffic$dau[df_traffic$yearmonth == month] / box_mau)*100),2),'%')

dbDisconnect(conn_chandra)