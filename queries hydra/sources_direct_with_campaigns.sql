-- tmp table : sessionslist
-- to remove the bots
select
  br || country_code as country
  ,session as cookie
  ,count(*) nbcount
into sessionslist
from hydra_verticals.web
where
  br in ('otomoto','autovit','standvirtual','otodom','storia','imovirtual')
  and session is not NULL
  and accept_cookies = 't'
  and server_date_day like '2017-10-%'
  and session_seq = 1
group by country,session

-- tmp table : sessionssources
-- get the sessions and their source in raw format
select
  session
  ,br || country_code as country
  ,case when platform_type is not null then platform_type
    else case when user_agent like '%Mobi%' then 'mobile-html5'
    else 'desktop' end end platform
  ,case
    when split_part(referer,'/', 3) like '%olx%' then 'olx'
    else case when (referer is null and invite_tracking_date = server_date) then 'dutm'||'--'||invite_source||'--'||invite_campaign
    else case when referer is null then 'direct'
    else case when (invite_tracking_date = server_date and invite_campaign != 'organic') then 'utm' || '--' || invite_source || '--' || invite_campaign
    else split_part(referer,'/',3)
  end end end end as traffic_source
  ,server_date
  ,server_date_day
  ,invite_source
  ,invite_campaign
  ,invite_tracking_date
  ,trackname
  ,referer
  ,session_long_crossdomain
  ,session_long
  ,session_long_seq
into sessionssources
from hydra_verticals.web dt
  left join sessionslist
  on dt.session = sessionslist.cookie
where
  dt.br in ('otomoto','autovit','standvirtual','otodom','storia','imovirtual')
  and dt.server_date_day like '2017-10-%'
  and dt.session_seq = 1
  and sessionslist.nbcount = 1

-- tmp table : sessions_sources
-- grouping the sources + provide campaigns.
SELECT
  session as cookie
  ,country
  ,platform
  ,server_date_day
  ,server_date
  ,case when split_part(traffic_source,'--',1) = 'utm' then 'campaigns'
    else case when split_part(traffic_source,'--',1) = 'dutm' then 'direct with campaigns'
    else case when traffic_source = 'direct' then 'direct'
    else case when traffic_source = 'olx' then 'olx'
    else case when traffic_source like '%allegro%' then 'allegro'
    else case when traffic_source like '%google%' or traffic_source like '%bing%' or traffic_source like '%yahoo%' then 'seo'
    else case when traffic_source like '%facebook%' or traffic_source like '%twitter%' or traffic_source like '%pinterest%' then 'social media'
    else case when traffic_source like '%otomoto%' then 'continuation of session'
    else case when traffic_source like '%autovit%' then 'continuation of session'
    else case when traffic_source like '%standvirtual%' then 'continuation of session'
    else case when traffic_source like '%otodom%' then 'continuation of session'
    else case when traffic_source like '%imovirtual%' then 'continuation of session'
    else case when traffic_source like '%storia%' then 'continuation of session'
    else 'other referrals'
  end end end end end end end end end end end end end source
  ,case when (split_part(traffic_source,'--',1) = 'utm' or split_part(traffic_source,'--',1) = 'dutm') then split_part(traffic_source,'--',2) end utm_source
  ,case when (split_part(traffic_source,'--',1) = 'utm' or split_part(traffic_source,'--',1) = 'dutm') then split_part(traffic_source,'--',3) end utm_medium
  ,case when (split_part(traffic_source,'--',1) = 'utm' or split_part(traffic_source,'--',1) = 'dutm') then split_part(traffic_source,'--',4) end utm_campaign
  ,case when traffic_source like '%www%' then split_part(traffic_source,'www.',2)
  else case when traffic_source not like 'utm%' then traffic_source else null end end ref_domain
  ,invite_source
  ,invite_campaign
  ,invite_tracking_date
  ,trackname
  ,referer
  ,session_long_crossdomain
  ,session_long
  ,session_long_seq
into sessions_sources
from jeremy_castan.sessionssources

-- tmp table : sessions_activity
-- map events to sessions
select
  session
  ,case when sum(case when trackname in ('reply_phone_show','reply_phone_call','reply_phone_sms','reply_message_sent') then 1 else 0 end) > 0 then 1 else 0 end as didreply
  ,case when sum(case when trackname = 'ad_page' then 1 else 0 end) > 0 then 1 else 0 end as didadview
  ,case when sum(case when trackname = 'listing' then 1 else 0 end) > 0 then 1 else 0 end as didbrowse
  ,sum(case when trackname in ('reply_phone_show','reply_phone_call','reply_phone_sms','reply_message_sent') then 1 else 0 end) as replies
  ,sum(case when trackname = 'ad_page' then 1 else 0 end) as adviews
  ,sum(case when trackname = 'listing' then 1 else 0 end) as browses
  into sessions_activity
  from hydra_verticals.web dt
left join sessionslist
    on dt.session = sessionslist.cookie
where
  dt.br in ('otomoto','autovit','standvirtual','otodom','storia','imovirtual')
  and dt.accept_cookies = 't'
  and dt.server_date_day like '2017-10-%'
  and sessionslist.nbcount = 1
group by dt.session

-- final table : sessions_table
-- map the source to the activity
select *
into sessions_table_verticals_cohort
from sessions_activity
left join sessions_sources
on sessions_activity.session = sessions_sources.cookie

-- work on sessions_table
select
server_date_day
,country
,platform
,source
,count(*) sessions
,sum(didreply) didreply
,sum(didadview) didadview
,sum(didbrowse) didbrowse
from sessions_table
group by server_date_day,country,platform,source