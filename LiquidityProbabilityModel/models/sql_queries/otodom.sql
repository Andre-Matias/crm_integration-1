
-- SECTION 1, nnls listings' attributes

select
    a.id as ad_id,
    a.region_id,
    a.category_id ,
    a.subregion_id,
    a.district_id,
    a.city_id,
    a.user_id,
    a.last_update_date,
    a.created_at_first,
    a.valid_to,
    a.status,
    len(a.title) as title_len,
    len(a.description) as description_len,
    a.params,
    a.map_address,
    a.private_business,
    a.riak_mapping as images_counter,
    a.paidads_id_index,
    a.paidads_id_payment,
    a.paidads_valid_to,
    a.was_paid_for_post,
    a.is_paid_for_post,
    a.export_olx_to,
    a.brand_program_id,
    a.user_quality_score,
    a.updated_at,
    a.street_name,
    a.street_id,
    a.panorama,
    a.mysql_search_rooms_num,
    a.mysql_search_m,
    a.mysql_search_price,
    a.mysql_search_price_per_m,
    a.movie,
    a.ad_quality_score,
    a.user_quality_score,
    a.map_lon,
    a.map_lat,
    a.net_ad_counted
    from
    olxgroupbi.livesync.verticals_ads a
    WHERE
    a.livesync_dbname = 'otodompl'
    and (cast(created_at_first as date) between '2017-06-01' and '2017-12-31')
    and a.net_ad_counted = 1;


-- SECTION 2, replies table

select
    a.ad_id as ad_id,
    a.created as ad_created,
    b.reply_id as reply_id,
    b.reply_parent_id as reply_parent_id,
    b.reply_sender_id as reply_sender_id,
    b.reply_posted as reply_posted_at
FROM
(select id as ad_id, created_at_first as created
from livesync.verticals_ads
where  cast(created_at_first as date) BETWEEN '2017-06-01' and '2017-12-31'
and livesync_dbname = 'otodompl') a
JOIN
(select
  id as reply_id,
  ad_id,
  parent_id as reply_parent_id,
  sender_id as reply_sender_id,
  posted as reply_posted,
  len(message) as len_reply_message
from livesync.verticals_answers
where livesync_dbname = 'otodompl'
and cast(posted as date) >= '2017-06-01') b
on a.ad_id = b.ad_id;


-- SECTION 3, get js events, within 1 week
SELECT
  a.ad_id,
  count(DISTINCT visitor_id) as unique_contacts_one_week,
  count(visitor_id) as total_leads_one_week
FROM
(select id as ad_id, extract(epoch from created_at_first) as created
from livesync.verticals_ads
where  cast(created_at_first as date) BETWEEN '2017-06-01' and '2017-12-31'
and livesync_dbname = 'otodompl') a
JOIN
    (SELECT
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201706
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201707
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201708
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201709
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201710
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201711
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201712
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-android'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201706
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201707
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201708
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201709
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201710
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201711
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_ios_201712
where
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)
and server_path = '/h/v-otodom-ios'
and item_id is not null)
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201706
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
)and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201707
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201708
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201709
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201710
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201711
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%')
UNION ALL
(select
server_date,
server_date_trunc,
item_id,
item_images_count,
eventname,
CASE
    WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201712
where
--trackpage = 'ad_page'and
eventname in
('reply_phone_show',
'reply_phone_call',
'reply_phone_sms',
'reply_phone_cancel',
'reply_message_sent',
'reply_message_click',
'reply_chat_sent'
) and
current_page LIKE '%oferta%'))
b
on a.ad_id = b.item_id
and (b.server_date - a.created)/(24*60*60) < 7
and (b.server_date - a.created)/(24*60*60) >= 0
--and a.livesync_dbname = 'otodompl'
GROUP BY ad_id;



-- [OPTIONAL] SECTION 4, views in one week

SELECT
  a.ad_id,
  count(DISTINCT visitor_id) as unique_vis,
  count(visitor_id) as total_vis
FROM
(select id as ad_id, extract(epoch from created_at_first) as created
from livesync.verticals_ads
where  cast(created_at_first as date) BETWEEN '2017-06-01' and '2017-09-23'
and livesync_dbname = 'otodompl') a
JOIN
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201709
where
trackpage = 'ad_page'
--and current_page LIKE '%anuncio%'
and brand = 'otodom'
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201708
where
trackpage = 'ad_page'
--and current_page LIKE '%anuncio%'
and brand = 'otodom')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201707
where
trackpage = 'ad_page'
--and current_page LIKE '%anuncio%'
and brand = 'otodom')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_web_201706
where
trackpage = 'ad_page'
--and current_page LIKE '%anuncio%'
and brand = 'otodom')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201709
where
server_path = '/h/v-otodom-android'
and item_id is not null
and trackpage = 'ad_page')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201708
where
server_path = '/h/v-otodom-android'
and item_id is not null
and trackpage = 'ad_page')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201707
where
server_path = '/h/v-otodom-android'
and item_id is not null
and trackpage = 'ad_page')
UNION ALL
(select
server_date_trunc, server_date, item_id,
CASE
    WHEN accept_cookies = TRUE THEN session_long
    ELSE ip_address + user_agent
    END
  AS visitor_id
from hydra.verticals_ninja_android_201706
where
server_path = '/h/v-otodom-android'
and item_id is not null
and trackpage = 'ad_page')
UNION ALL
(select
  server_date_trunc, server_date, item_id,
  CASE
      WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
      END
    AS visitor_id
  from hydra.verticals_ninja_ios_201709
  where
  server_path = '/h/v-otodom-ios'
  and item_id is not null
  and trackpage = 'ad_page')
  UNION ALL
  (select
  server_date_trunc, server_date, item_id,
  CASE
      WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
      END
    AS visitor_id
  from hydra.verticals_ninja_ios_201708
  where
  server_path = '/h/v-otodom-ios'
  and item_id is not null
  and trackpage = 'ad_page')
  UNION ALL
  (select
  server_date_trunc, server_date, item_id,
  CASE
      WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
      END
    AS visitor_id
  from hydra.verticals_ninja_ios_201707
  where
  server_path = '/h/v-otodom-ios'
  and item_id is not null
  and trackpage = 'ad_page')
  UNION ALL
  (select
  server_date_trunc, server_date, item_id,
  CASE
      WHEN accept_cookies = TRUE THEN session_long
      ELSE ip_address + user_agent
      END
    AS visitor_id
  from hydra.verticals_ninja_ios_201706
  where
  server_path = '/h/v-otodom-ios'
  and item_id is not null
  and trackpage = 'ad_page')
  ) b
on a.ad_id = b.item_id
and (b.server_date - a.created)/(24*60*60) < 7
and (b.server_date - a.created)/(24*60*60) >= 0
GROUP BY ad_id;












