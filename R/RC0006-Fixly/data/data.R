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
                    group by c.new_id, name_en ;
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
                                  group by  user_id, email, phone, c.new_id, name_en, b.bucket ")
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
                                   order by bucket, categoryid ")

df_teste_daily <- dbFetch(res)
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "select bucket,
                             count(distinct user_id)
                             from odl_global_verticals.vert_services_fixly_buckets_segment
                             group by bucket ")

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
                                order by 6 desc, 4 desc ")

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


df_dailyDB <- dbSendQuery(conn_chandra,
                          "with trackers as (
    SELECT *
    FROM rdl_vertical_services.trackers_general_traffic
    WHERE tracker = 'ga'
),
registered_users as( with active_profs as (
select registration_date,
 count(distinct professional_id)
from odl_global_verticals.vert_services_fixly_prof_l3_city
group by registration_date)
select registration_date,
 sum(actp.count) over(order by actp.registration_date rows unbounded preceding) registered_professionals
from active_profs actp
ORDER BY registration_date ASC)
SELECT
  trackers.date,
  trackers.users,
  trackers.bounces,
  trackers.pageviews,
  trackers.sessions,
  ISNULL(registered_users.registered_professionals, 0) registered_professionals
FROM trackers
LEFT JOIN registered_users ON trackers.date = registered_users.registration_date
                          ORDER BY date ASC")

df_dailyDB <- dbFetch(df_dailyDB)

df_monthlyDB <- dbSendQuery(conn_chandra,
                            "with sums as (
  SELECT
                            to_char(date_posted, 'YYYYMM') monthyear,
                            count(DISTINCT user_id) active_users,
                            count(*) as nb_requests,
                            avg(rating_professional) average_pro_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
),
                            satisfied_req as (
                            SELECT
                            to_char(date_satisfied, 'YYYYMM') monthyear,
                            count(DISTINCT user_id) satisfied_users,
                            count(*) satisfied_requests
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE date_satisfied IS NOT NULL
                            GROUP BY monthyear
                            ),
                            answ_users as (
                            SELECT
                            to_char(date_first_reply, 'YYYYMM') monthyear,
                            count(DISTINCT user_id) answered_users
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            WHERE qty_answers > 0
                            GROUP BY monthyear
                            ),
                            u_avg_rating as (
                            SELECT
                            to_char(date_satisfied, 'YYYYMM') monthyear,
                            avg(rating_professional) avg_user_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            p_avg_rating as (
                            SELECT
                            to_char(date_satisfied, 'YYYYMM') monthyear,
                            avg(rating_user) avg_pro_rating
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            p_raters as (
                            SELECT
                            to_char(date_satisfied, 'YYYYMM') monthyear,
                            count(DISTINCT case when rating_user = 1 then user_id else null end) p_raters_1stars,
                            count(DISTINCT case when rating_user = 2 then user_id else null end) p_raters_2stars,
                            count(DISTINCT case when rating_user = 3 then user_id else null end) p_raters_3stars,
                            count(DISTINCT case when rating_user = 4 then user_id else null end) p_raters_4stars,
                            count(DISTINCT case when rating_user = 5 then user_id else null end) p_raters_5stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            u_raters as(
                            SELECT
                            to_char(date_satisfied, 'YYYYMM') monthyear,
                            count(DISTINCT case when rating_professional = 1 then user_id else null end) u_raters_1stars,
                            count(DISTINCT case when rating_professional = 2 then user_id else null end) u_raters_2stars,
                            count(DISTINCT case when rating_professional = 3 then user_id else null end) u_raters_3stars,
                            count(DISTINCT case when rating_professional = 4 then user_id else null end) u_raters_4stars,
                            count(DISTINCT case when rating_professional = 5 then user_id else null end) u_raters_5stars
                            FROM odl_global_verticals.vert_services_fixly_requests_l3_city
                            GROUP BY monthyear
                            ),
                            traffic_m as (
                            SELECT
                            left(yearmonth,4)+right(yearmonth,2) date,
                            users
                            FROM rdl_vertical_services.trackers_monthly_traffic
                            WHERE tracker = 'ga'
                            ),
                            traffic_d as (
                            select to_char(date, 'YYYYMM') monthyear,
                            sum(bounces)*1.0/sum(sessions) * 100 bounce_rate,
                            avg(users) avg_dau
                            FROM rdl_vertical_services.trackers_general_traffic
                            where tracker='ga'
                            group by to_char(date, 'YYYYMM')
                            ),
                            active_profs as (
                            select to_char(registration_date, 'YYYYMM') monthyear,
                            count(distinct professional_id) registered_professionals
                            from odl_global_verticals.vert_services_fixly_prof_l3_city
                            group by to_char(registration_date, 'YYYYMM')
                            )
                            SELECT
                            a.date,
                            a.users,
                            b.satisfied_users,
                            b.satisfied_requests,
                            c.answered_users,
                            d.avg_user_rating,
                            e.avg_pro_rating,
                            f.active_users,
                            f.nb_requests,
                            f.average_pro_rating,
                            sum(actp.registered_professionals) over(order by a.date rows unbounded preceding) registered_professionals,
                            (sum(actp.registered_professionals) over(order by a.date rows unbounded preceding)*1.0) / a.users * 100 registered_professionals_mau,
                            br.bounce_rate,
                            br.avg_dau*1.0/a.users*100 stickiness,
                            pr.p_raters_1stars,
                            pr.p_raters_2stars,
                            pr.p_raters_3stars,
                            pr.p_raters_4stars,
                            pr.p_raters_5stars,
                            ur.u_raters_1stars,
                            ur.u_raters_2stars,
                            ur.u_raters_3stars,
                            ur.u_raters_4stars,
                            ur.u_raters_5stars
                            FROM traffic_m a
                            LEFT JOIN satisfied_req b ON a.date = b.monthyear
                            LEFT JOIN answ_users c ON a.date = c.monthyear
                            LEFT JOIN u_avg_rating d ON a.date = d.monthyear
                            LEFT JOIN p_avg_rating e ON a.date = e.monthyear
                            LEFT JOIN sums f ON a.date = f.monthyear
                            LEFT JOIN active_profs actp ON a.date = actp.monthyear
                            LEFT JOIN traffic_d br ON a.date = br.monthyear
                            LEFT JOIN p_raters pr ON a.date = pr.monthyear
                            LEFT JOIN u_raters ur ON a.date = ur.monthyear
                            ORDER BY a.date ASC")
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
                           sum(qty_approved_quotes) approved_quotes,
                           count(DISTINCT professional_id) nb_pros
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city),
                           active_pros as (
                           SELECT
                           count(DISTINCT professional_id) active_pros
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_quotes > 0
                           ),
                           approved_pros as (
                           SELECT
                           count(DISTINCT professional_id) approved_pros
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_approved_quotes > 0
                           ),
                           user_table as(
                           SELECT
                           sum(qty_answered_requests) answered_requests,
                           sum(qty_satisfied_requests) satisfied_requests
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city),
                           active_users as (
                           SELECT
                           count(DISTINCT user_id) active_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_requests > 0
                           ),
                           answered as (
                           SELECT
                           count(DISTINCT user_id) answered_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_answered_requests > 0
                           ),
                           satisfied as (
                           SELECT
                           count(DISTINCT user_id) satisfied_users
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_satisfied_requests > 0
                           ),
                           u_raters1 as (
                           SELECT
                           count(DISTINCT user_id) u_raters_1star
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_1star_rated > 0
                           ),
                           u_raters2 as (
                           SELECT
                           count(DISTINCT user_id) u_raters_2stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_2star_rated > 0
                           ),
                           u_raters3 as (
                           SELECT
                           count(DISTINCT user_id) u_raters_3stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_3star_rated > 0
                           ),
                           u_raters4 as (
                           SELECT
                           count(DISTINCT user_id) u_raters_4stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_4star_rated > 0
                           ),
                           u_raters5 as (
                           SELECT
                           count(DISTINCT user_id) u_raters_5stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_5star_rated > 0
                           ),
                           p_raters1 as (
                           SELECT
                           count(DISTINCT professional_id) p_raters_1star
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_1star > 0
                           ),
                           p_raters2 as (
                           SELECT
                           count(DISTINCT professional_id) p_raters_2stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_2star > 0
                           ),
                           p_raters3 as (
                           SELECT
                           count(DISTINCT professional_id) p_raters_3stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_3star > 0
                           ),
                           p_raters4 as (
                           SELECT
                           count(DISTINCT professional_id) p_raters_4stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_4star > 0
                           ),
                           p_raters5 as (
                           SELECT
                           count(DISTINCT professional_id) p_raters_5stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_5star > 0
                           ),
                           p_raters4or5 as(
                           SELECT
                           count(DISTINCT professional_id) p_raters_4or5stars
                           FROM odl_global_verticals.vert_services_fixly_prof_l3_city
                           WHERE qty_users_rated_5star > 0 OR qty_users_rated_4star > 0
                           ),
                           u_raters4or5 as(
                           SELECT
                           count(DISTINCT user_id) u_raters_4or5stars
                           FROM odl_global_verticals.vert_services_fixly_user_l3_city
                           WHERE qty_professionals_5star_rated > 0 OR qty_professionals_4star_rated > 0
                           )
                           SELECT *
                           FROM
                           req_table,pro_table,user_table,answered,satisfied,active_users,active_pros,approved_pros,
                           u_raters1,u_raters2,u_raters3,u_raters4,u_raters5,u_raters4or5,
                           p_raters1,p_raters2,p_raters3,p_raters4,p_raters5,p_raters4or5")

df_globalDB <- dbFetch(df_globalDB)

df_categoriesDB <- dbSendQuery(conn_chandra,
                               "select category_l1_desc, category_l2_desc, count(distinct professional_id)
from odl_global_verticals.vert_services_fixly_prof_l3_city
group by category_l1_desc, category_l2_desc")

df_categoriesDB <- dbFetch(df_categoriesDB)

df_citiesDB <- dbSendQuery(conn_chandra,
                           "select city_desc, count(distinct professional_id)
from odl_global_verticals.vert_services_fixly_prof_l3_city
                           group by city_desc")

df_citiesDB <- dbFetch(df_citiesDB)
dbDisconnect(conn_chandra)

### adding columns in tables
df_monthlyDB$yearmonth <- substr(gsub('-','',df_monthlyDB$date),0,6)
df_dailyDB$yearmonth <- substr(gsub('-','',df_dailyDB$date),0,6)
df_dailyDB$bounce_rate <- round((df_dailyDB$bounces / df_dailyDB$sessions), 2)
df_dailyDB <- merge(df_dailyDB,df_monthlyDB[,c('yearmonth','users')], by = 'yearmonth')
df_dailyDB <- rename(df_dailyDB, c('users.x'='dau','users.y'='mau'))
df_dailyDB$stickiness <- round((df_dailyDB$dau / df_dailyDB$mau)*100, 2)

### getting the boxes
month <- substr(gsub('-','', as.character(Sys.Date()-1)),0,6)
Sys.setlocale("LC_TIME", "C")
current_month <- months(as.Date(Sys.time()))
dash <- '-'

#common boxes
box_mau <- df_monthlyDB$users[df_monthlyDB$date == month]
box_bounce_rate <- paste0(round(df_monthlyDB$bounce_rate[df_monthlyDB$date == month], 1),'%')
box_stickiness <- paste0(round(df_monthlyDB$stickiness[df_monthlyDB$date == month], 1),'%')

# professionals - boxes
sum_p_raters <- rowSums(df_globalDB[1,c('p_raters_1star','p_raters_2stars','p_raters_3stars','p_raters_4stars','p_raters_5stars')])
sum_p_rates <- df_globalDB[1,c('p_raters_1star')] + (2*df_globalDB[1,c('p_raters_2stars')]) + (3*df_globalDB[1,c('p_raters_3stars')]) + (4*df_globalDB[1,c('p_raters_4stars')]) + (5*df_globalDB[1,c('p_raters_5stars')])
box_registered_pros <- df_globalDB[1,c('nb_pros')]
box_registered_pros_mau <- paste0(round(df_monthlyDB$registered_professionals_mau[df_monthlyDB$date == month], 1),'%')
box_quotes <- df_globalDB[1,c('quotes')]
box_approved_quotes <- df_globalDB[1,c('approved_quotes')]
box_approved_quotes_per_active <- box_approved_quotes / df_globalDB[1,c('active_pros')]
box_bounces_payment <- 0
box_avg_nb_quotes <- box_quotes / box_approved_quotes
box_rated_pros <- df_globalDB[1,c('p_raters_4or5stars')]
box_avg_p_rating <- sum_p_rates / sum_p_raters

# professionals - category
df_l1cat <- aggregate(df_categoriesDB$count, by = list(category=df_categoriesDB$category_l1_desc), FUN = sum)

# biznes
catL2_Biznes <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Biznes'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Biznes']
)
htmlCatL2Bizness <- paste0('<div>',as.character(htmlTable(catL2_Biznes)),'</div>')
htmlCatL2Bizness <- gsub('\n','',htmlCatL2Bizness)

# dom i buro
catL2_domIburo <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Dom i biuro'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Dom i biuro']
)
htmlCatL2domIburo <- paste0('<div>',as.character(htmlTable(catL2_domIburo)),'</div>')
htmlCatL2domIburo <- gsub('\n','',htmlCatL2domIburo)

# edukacja
catL2_edukacja <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Edukacja'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Edukacja']
)
htmlCatL2edukacja <- paste0('<div>',as.character(htmlTable(catL2_edukacja)),'</div>')
htmlCatL2edukacja <- gsub('\n','',htmlCatL2edukacja)

# pozostale
catL2_pozostale <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Pozostale'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Pozostale']
)
htmlCatL2pozostale <- paste0('<div>',as.character(htmlTable(catL2_pozostale)),'</div>')
htmlCatL2pozostale <- gsub('\n','',htmlCatL2pozostale)

# zdrowie i uroda
catL2_zdrowieIuroda <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Zdrowie i Uroda'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Zdrowie i Uroda']
)
htmlCatL2zdrowieIuroda <- paste0('<div>',as.character(htmlTable(catL2_zdrowieIuroda)),'</div>')
htmlCatL2zdrowieIuroda <- gsub('\n','',htmlCatL2zdrowieIuroda)

# motoryacja
catL2_motoryacja <- data.frame(
  l2_cat = df_categoriesDB$category_l2_desc[df_categoriesDB$category_l1_desc == 'Motoryacja'],
  count = df_categoriesDB$count[df_categoriesDB$category_l1_desc == 'Motoryacja']
)
htmlCatL2motoryacja <- paste0('<div>',as.character(htmlTable(catL2_motoryacja)),'</div>')
htmlCatL2motoryacja <- gsub('\n','',htmlCatL2motoryacja)

# merge tables
df_l1cat_assignHtml <- data.frame(
  category = c('Biznes','Dom i biuro','Edukacja','Pozostale','Zdrowie i Uroda','Motoryzacja'),
  x.html.tooltip = c(htmlCatL2Bizness,htmlCatL2domIburo,htmlCatL2edukacja,htmlCatL2pozostale,htmlCatL2zdrowieIuroda,htmlCatL2motoryacja)
)
catL1_chart <- merge(df_l1cat_assignHtml,df_l1cat, by = 'category')

# users
sum_u_raters <- rowSums(df_globalDB[1,c('u_raters_1star','u_raters_2stars','u_raters_3stars','u_raters_4stars','u_raters_5stars')])
sum_u_rates <- df_globalDB[1,c('u_raters_1star')] + (2*df_globalDB[1,c('u_raters_2stars')]) + (3*df_globalDB[1,c('u_raters_3stars')]) + (4*df_globalDB[1,c('u_raters_4stars')]) + (5*df_globalDB[1,c('u_raters_5stars')])
box_active_users <- 0
box_nb_requests <- df_globalDB[1,c('total_requests')]
box_posting_users <- df_globalDB[1,c('active_users')]
box_users_w_x_requests <- 0
box_satisfied_users <- df_globalDB[1,c('satisfied_users')]
box_total_request <- 0
box_rated_users <- 0
box_avg_u_rating <- sum_u_rates / sum_u_raters
