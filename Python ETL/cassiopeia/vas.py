import psycopg2


class redshift:
	def __init__(self, host, port, db, user, password):
		self.redshift_host = host
		self.redshift_port = port
		self.redshift_db = db
		self.redshift_user = user
		self.redshift_password = password

	def connect(self):
		self.con = psycopg2.connect(dbname=self.redshift_db, host=self.redshift_host, port=self.redshift_port, user=self.redshift_user, password=self.redshift_password)
		self.cursor = self.con.cursor()

	def disconnect(self):
		self.cursor.close()
		self.con.close()

	def runQuery(self,query):
		self.cursor.execute(query)
		self.con.commit()


def runquery():

	rs = redshift('10.101.5.237','5671','main','miguel_chin','Yamato4life')

	rs.connect()

	rs.runQuery("INSERT INTO miguel_chin.vas_dataset  \
			(WITH status_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  status, \
			  livesync_dbname, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and status is not null \
			      and status != '' \
			), \
			status_latest AS ( \
			select * from status_order where row_number = 1 \
			), \
			created_at_first_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  created_at_first, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and created_at_first is not null \
			), \
			created_at_first_latest AS ( \
			SELECT * FROM created_at_first_order WHERE row_number = 1 \
			), \
			created_at_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  created_at, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and created_at is not null \
			), \
			created_at_latest AS ( \
			SELECT * FROM created_at_order WHERE row_number = 1 \
			), \
			export_olx_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  export_olx_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and export_olx_to is not null \
			), \
			export_olx_to_latest AS ( \
			SELECT * FROM export_olx_to_order WHERE row_number = 1 \
			), \
			bump_date_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  bump_date, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and bump_date is not null \
			), \
			bump_date_latest AS ( \
			SELECT * FROM bump_date_order WHERE row_number = 1 \
			), \
			valid_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  valid_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and valid_to is not null \
			), \
			valid_to_latest AS ( \
			SELECT * FROM valid_to_order WHERE row_number = 1 \
			), \
			highlight_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \ \
			  highlight_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and highlight_to is not null \
			), \
			highlight_to_latest AS ( \
			SELECT * FROM highlight_to_order WHERE row_number = 1 \
			), \
			paidads_valid_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  paidads_valid_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and paidads_valid_to is not null \
			), \
			paidads_valid_to_latest AS ( \
			SELECT * FROM paidads_valid_to_order WHERE row_number = 1 \
			), \
			ad_homepage_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  ad_homepage_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and ad_homepage_to is not null \
			), \
			ad_homepage_to_latest AS ( \
			SELECT * FROM ad_homepage_to_order WHERE row_number = 1 \
			), \
			ad_bighomepage_to_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  ad_bighomepage_to, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and ad_bighomepage_to is not null \
			), \
			ad_bighomepage_to_latest AS ( \
			SELECT * FROM ad_bighomepage_to_order WHERE row_number = 1 \
			), \
			private_business_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  private_business, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and private_business is not null \
			      and private_business != '' \
			), \
			private_business_latest AS ( \
			SELECT * FROM private_business_order WHERE row_number = 1 \
			), \
			phone_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  phone, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and phone is not null \
			      and phone != '' \
			), \
			phone_latest AS ( \
			SELECT * FROM phone_order WHERE row_number = 1 \
			), \
			region_id_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  region_id, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and region_id is not null \
			), \
			region_id_latest AS ( \
			SELECT * FROM region_id_order WHERE row_number = 1 \
			), \
			city_id_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  city_id, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and city_id is not null \
			), \
			city_id_latest AS ( \
			SELECT * FROM city_id_order WHERE row_number = 1 \
			), \
			category_id_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  category_id, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and category_id is not null \
			), \
			category_id_latest AS ( \
			SELECT * FROM category_id_order WHERE row_number = 1 \
			), \
			params_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  params, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and params is not null \
			      and params != '' \
			), \
			params_latest AS ( \
			SELECT * FROM params_order WHERE row_number = 1 \
			), \
			title_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  title, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and title is not null \
			      and title != '' \
			), \
			title_latest AS ( \
			SELECT * FROM title_order WHERE row_number = 1 \
			), \
			description_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  description, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc, ""primary"" DESC) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      and description is not null \
			      and description != '' \
			), \
			description_latest AS ( \
			SELECT * FROM description_order WHERE row_number = 1 \
			), \
			number_photos_order as ( \
			SELECT \
			  id, \
			  changed_at, \
			  db_atlas_verticals.count_images(COALESCE(riak_mapping, 0)) as number_photos, \
			  ROW_NUMBER() OVER (PARTITION BY id ORDER BY changed_at desc) as row_number \
			FROM miguel_chin.verticals_ads_history_from_source \
			WHERE table_name = 'ads_history_otomotopl' \
			      AND date(changed_at) <= '{0}' \
			      AND db_atlas_verticals.count_images(COALESCE(riak_mapping, 0)) is not null \
			      AND riak_mapping is not null \
			), \
			number_photos_latest AS ( \
			SELECT * FROM number_photos_order WHERE row_number = 1 \
			) \
			SELECT \
			  'otomoto.pl', \
			  '{0}' as date, \
			  status_latest.id, \
			  created_at_first as creation_date, \
			  created_at as ad_reactivation_date, \
			  status, \
			  valid_to, \
			  bump_date, \
			  CASE \
			    WHEN highlight_to >= '{0}' THEN TRUE \
			    ELSE FALSE \
			  END as highlight, \
			  CASE \
			    WHEN export_olx_to >= '{0}' THEN TRUE \
			    ELSE FALSE \
			  END as olx, \
			  CASE \
			    WHEN paidads_valid_to >= '{0}' THEN TRUE \
			    ELSE FALSE \
			  END as topads, \
			  CASE \
			    WHEN ad_homepage_to >= '{0}' THEN TRUE \
			    ELSE FALSE \
			  END as homepage, \
			  CASE \
			    WHEN ad_bighomepage_to >= '{0}' THEN TRUE \
			    ELSE FALSE \
			  END as bighomepage, \
			  private_business as seller_type, \
			  CASE \
			    WHEN phone IS NULL THEN FALSE \
			    WHEN phone IS NOT NULL THEN TRUE \
			  END as has_phone, \
			  region_id, \
			  city_id, \
			  category_id, \
			  params, \
			  title, \
			  description, \
			  number_photos, \
			  NULLIF(REGEXP_SUBSTR(CASE \
			                      /* Panamera json */ WHEN params LIKE '{%""price"":%}' THEN JSON_EXTRACT_PATH_TEXT(params, 'price') \
			                      /* Atlas format  */ WHEN params LIKE '%price<=>%'   THEN REGEXP_SUBSTR(params, 'price<=>[0-9\.]+') \
			                  END, '[0-9\.]+'), '') :: NUMERIC(38,3) AS price \
			FROM status_latest \
			LEFT JOIN created_at_first_latest ON status_latest.id = created_at_first_latest.id \
			LEFT JOIN created_at_latest ON status_latest.id = created_at_latest.id \
			LEFT JOIN export_olx_to_latest ON status_latest.id = export_olx_to_latest.id \
			LEFT JOIN bump_date_latest ON status_latest.id = bump_date_latest.id \
			LEFT JOIN valid_to_latest ON status_latest.id = valid_to_latest.id \
			LEFT JOIN highlight_to_latest ON status_latest.id = highlight_to_latest.id \
			LEFT JOIN paidads_valid_to_latest ON status_latest.id = paidads_valid_to_latest.id \
			LEFT JOIN ad_homepage_to_latest ON status_latest.id = ad_homepage_to_latest.id \
			LEFT JOIN ad_bighomepage_to_latest ON status_latest.id = ad_bighomepage_to_latest.id \
			LEFT JOIN private_business_latest ON status_latest.id = private_business_latest.id \
			LEFT JOIN phone_latest ON status_latest.id = phone_latest.id \
			LEFT JOIN region_id_latest ON status_latest.id = region_id_latest.id \
			LEFT JOIN city_id_latest ON status_latest.id = city_id_latest.id \
			LEFT JOIN category_id_latest ON status_latest.id = category_id_latest.id \
			LEFT JOIN params_latest ON status_latest.id = params_latest.id \
			LEFT JOIN title_latest ON status_latest.id = title_latest.id \
			LEFT JOIN description_latest ON status_latest.id = description_latest.id \
			LEFT JOIN number_photos_latest ON status_latest.id = number_photos_latest.id \
			WHERE \
			 created_at_first is not NULL \
			 AND params is not NULL \
			 AND seller_type is not NULL \
			 AND NOT (status = 'active' and valid_to <='{0}') \
			) \
		")

	

	rs.disconnect()

	print("Process completed.")

def main():
	runquery()


if __name__ == '__main__':
    main()