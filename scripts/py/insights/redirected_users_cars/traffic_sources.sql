-- create the sessionslist table to get the sessions
-- here we only want the first event of the session and the related session ID
-- I use the count to filter bots. If a session has more than 1 first event, then it's a bot
-- it will be one of the filters in the next script

select
br || country_code as country
,session as cookie
,count(*) nbcount
into sessionslist
from hydra_verticals.web
where
br in ('otomoto','autovit','standvirtual')
and session is not NULL
and accept_cookies = 't'
and server_date_day like '2017-12-%'
and session_seq = 1
group by country,session

-- once we have this group, we can start getting the referrers and apply a first layer on sources
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
dt.br in ('otomoto','autovit','standvirtual')
and dt.server_date_day like '2017-12-%'
and dt.session_seq = 1
and sessionslist.nbcount = 1

-- then I affine the traffic source rules to get understandable labels
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
else case when traffic_source like '%facebook%' or traffic_source like '%twitter%' or traffic_source like '%pinterest%' or traffic_source like '%whatsapp%' then 'social media'
else case when traffic_source like '%otomoto%' then 'continuation of session'
else case when traffic_source like '%autovit%' then 'continuation of session'
else case when traffic_source like '%standvirtual%' then 'continuation of session'
else 'other referrals'
end end end end end end end end end end source
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


-- Then I create another table to get the sessions which have...
-- ... replied, made adview, browsed, and the related # of replies, adview, browses
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
and dt.server_date_day like '2017-12-%'
and sessionslist.nbcount = 1
group by dt.session


-- Then I left join to get a table where
-- 1 row is a session and I know the source and if it has converted or not
-- I can now work on the sessions_verticals table
-- And remove the other tables because I won't care
select *
into sessions_verticals
from sessions_activity
left join sessions_sources
on sessions_activity.session = sessions_sources.cookie