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
                   from public.fixly_buckets b
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
                   c.new_id as categoryid,
                   name_en as \"Category\",
                   sum(nnls) as \"NNL 6 months\",
                   sum(active_ads) as \"Active Ads\",
                   sum(reve1m) as \"Revenue 1 Month\",
                   sum(reve1m) as \"Revenue 6 Months\",
                   b.bucket,
                   'FALSE' as Onboarded
                   from public.fixly_buckets b
                   join md_category_english c
                   on b.category_id=c.id
                   where email is not null
                   group by  user_id, email, c.new_id, name_en, b.bucket")
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
                   from public.fixly_buckets b
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
                   from public.fixly_buckets
                   group by bucket")

totalUsersPerBucket <-dbFetch(res)

totalUsersPerBucket$Pct <- round((totalUsersPerBucket$count / sum(totalUsersPerBucket$count))*100,0)
totalUsersPerBucket$label <- paste0(totalUsersPerBucket$bucket, ", ", totalUsersPerBucket$Pct, "%")
dbClearResult(dbListResults(conn_chandra)[[1]])

res <- dbSendQuery(conn_chandra, "
                                with users as(
                   select count(DISTINCT user_id) total_users from public.fixly_buckets
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
                   OVER () AS total_vas
                   FROM public.fixly_buckets
                   where email is not null
                   )
                   select
                   bucket,
                   count(DISTINCT user_id) as \"# professionals\",
                   convert(varchar,round(count(DISTINCT user_id)*100.0 /(total_users),0))+ '% ' as \"% professionals\",
                   sum(active_ads) as \"# active ads\",
                   convert(varchar,round(sum(active_ads)*100.0/max(total_active_ads),0)) + '% ' as \"% active ads\",
                   sum(reve1m) as \"VAS generated revenue\",
                   convert(varchar,round(sum(reve1m)*100.0/max(total_vas),0)) + '% ' as \"%VAS generated revenue\"
                   from buckets, users
                   group by bucket, total_users")

df_desc <- dbFetch(res)
dbClearResult(dbListResults(conn_chandra)[[1]])

dbDisconnect(conn_chandra)

df_unpvot <- melt(df_teste, id.vars = c("service","category","categoryid"))

df_desc$Definition <- c("More than 1 active ad and at least 1 VAS purchase within last month",
                        "1 active ad and at least 1 VAS purchase within last month",
                        "At least 1 active ad, 0 payments within last month, at least 1 payment within last 6 month + 0 active ads and at least 1 payment within last 6 months",
                        "At least 1 active ad and 0 payments within last 6 month",
                        "Zero ads active, zero payments within last 6 months, and at least 1 ad added within last 6 months")