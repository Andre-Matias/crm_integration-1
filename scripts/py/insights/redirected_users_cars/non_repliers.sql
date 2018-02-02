-- I do the same as repliers_pl but here for users who didn't answer during their session
WITH cd_cookie_otomoto AS (
    SELECT session_long_crossdomain
    FROM sessions_verticals
    WHERE didreply = 0 AND country = 'otomotoPL' AND source = 'olx'
          AND server_date_day BETWEEN '2017-12-01' AND '2017-12-07'
)
SELECT
  'otomoto'                          site,
  session_long_crossdomain           cd_cookie,
  CASE WHEN user_agent LIKE '%Mobi%'
    THEN 'mobile'
  ELSE 'desktop' END                 platform,
  server_date_day                    event_date,
  server_date                        event_ts,
  trackname                          event,
  ad_id,
  CASE WHEN referer LIKE '%olx%'
    THEN 'olx'
  ELSE CASE WHEN referer LIKE '%otomoto%'
    THEN 'otomoto'
       ELSE CASE WHEN referer IS NULL
         THEN 'direct'
            ELSE 'other' END END END source
INTO stream_otomoto_non_repliers
FROM hydra_verticals.web v
WHERE br = 'otomoto'
      AND accept_cookies = 't'
      AND server_date_day BETWEEN '2017-12-01' AND '2017-12-07'
      AND trackname IN
          ('reply_phone_show', 'reply_message_click', 'reply_phone_call', 'reply_phone_sms', 'ad_page', 'listing', 'favourite_ad_click')
      AND exists(
          SELECT *
          FROM cd_cookie_otomoto
          WHERE cd_cookie_otomoto.session_long_crossdomain = v.session_long_crossdomain
      )

-- Get events from Otomoto repliers in OLX PL
WITH cd_cookie_otomoto AS (
    SELECT session_long_crossdomain
    FROM sessions_verticals
    WHERE didreply = 0 AND country = 'otomotoPL' AND source = 'olx'
          AND server_date_day BETWEEN '2017-12-01' AND '2017-12-07'
)
SELECT
  'olx'                     site,
  multidomain_session_long  cd_cookie,
  CASE WHEN user_agent LIKE '%Mobi%'
    THEN 'mobile'
  ELSE 'desktop' END        platform,
  server_date_day           event_date,
  server_date               event_ts,
  action_type               event,
  ad_id,
  CASE WHEN referer LIKE '%olx%'
    THEN 'olx'
  ELSE CASE WHEN referer IS NULL
    THEN 'direct'
       ELSE 'other' END END source
INTO stream_olxpl_non_repliers
FROM hydra.web h
WHERE country_code = 'PL'
      AND accept_cookies = 't'
      AND server_date_day BETWEEN '2017-12-01' AND '2017-12-07'
      AND cat_l1_id IN (5, 757)
      AND action_type IN
          ('reply_phone_1step', 'reply_chat_sent', 'reply_2step_call', 'reply_2step_sms', 'ad_page', 'listing', 'favourite_ad_click')
      AND exists(
          SELECT *
          FROM cd_cookie_otomoto
          WHERE cd_cookie_otomoto.session_long_crossdomain = h.multidomain_session_long
      )


-- rbind both tables
CREATE TABLE jeremy_castan.stream_pl_non_repliers AS (
  SELECT *
  FROM stream_otomoto_non_repliers
  UNION ALL
  SELECT *
  FROM stream_olxpl_non_repliers
)

-- create daily tables for Python
-- because it's too big amount of data
-- and it crashed redshift
SELECT
  s.*,
  row_number()
  OVER (
    ORDER BY s.cd_cookie, s.event_ts
    ) row_nb
INTO pl_non_repliers_02122017
FROM stream_pl_non_repliers s
WHERE event_date = '2017-12-02'

-- get the stream ordered (example)
SELECT *
FROM stream_pl_repliers
WHERE event_date = '2017-12-01'
ORDER BY cd_cookie, event_ts ASC