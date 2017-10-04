import psycopg2
import simplejson as json
import sys
from boto.s3.connection import S3Connection, Bucket, Key
from datetime import date, timedelta

def getConnection(conf_file):
  	data = json.load(open(conf_file))	
  	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return 'aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s' \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def deletePreviousS3Files(conf_file):
	conf = json.load(open(conf_file))
	key = conf['s3_key']
	skey = conf['s3_skey']

	conn = S3Connection(key, skey)
	b = Bucket(conn, 'verticals-raw-data')
	for x in b.list(prefix = 'Ajani/Temp/latam'):
		x.delete()

def getUnloadQuery():
	query = "UNLOAD (' " \
			"SELECT distinct A.country_id, A.time_id, B.featured_time_id,A.user_id, A.item_id "\
			"FROM latam_sandbox.latam_ft_d_items_with_paid_products AS B, ( "\
			"SELECT "\
			"A.country_id, "\
			"A.user_id, "\
			"A.time_id, "\
			"A.item_id "\
			"FROM ods_naspers.ft_h_listing AS A "\
			"WHERE A.country_id IN (32, 170, 218, 604) "\
			"AND A.category_l2_id = 378 "\
			"AND A.device_source_id = 27 "\
			"AND A.platform_id = 1 "\
			"AND A.live_id = 1 "\
			") AS A "\
			"WHERE A.user_id = B.user_id "\
			"AND A.country_id = B.country_id "\
			"AND B.product_id IN (9,10,1,2,3,7,8) " \
			"') "\
			"to 's3://verticals-raw-data/Ajani/Temp/latam/data_' "\
			"CREDENTIALS %s "\
			"ESCAPE "\
			"GZIP "\
			"ALLOWOVERWRITE "\
			"manifest;"
	return query

def getLoadQuery():
	copy_query = "TRUNCATE TABLE verticals_bi.aux_latam_ppu; "\
					"COPY verticals_bi.aux_latam_ppu from 's3://verticals-raw-data/Ajani/Temp/latam/data_manifest' "\
					"CREDENTIALS %s "\
					"ESCAPE "\
					"GZIP "\
					"manifest;"
	return copy_query

def getUnloadDailyQuery():
	query = "UNLOAD (' " \
			"SELECT distinct A.country_id, A.time_id, B.featured_time_id,A.user_id, A.item_id "\
			"FROM latam_sandbox.latam_ft_d_items_with_paid_products AS B, ( "\
			"SELECT "\
			"A.country_id, "\
			"A.user_id, "\
			"A.time_id, "\
			"A.item_id "\
			"FROM ods_naspers.ft_h_listing AS A "\
			"WHERE A.country_id IN (32, 170, 218, 604) "\
			"AND A.category_l2_id = 378 "\
			"AND A.device_source_id = 27 "\
			"AND A.platform_id = 1 "\
			"AND A.live_id = 1 "\
			"AND A.time_id = '%s' "\
			") AS A "\
			"WHERE A.user_id = B.user_id "\
			"AND A.country_id = B.country_id "\
			"AND B.featured_time_id = '%s' "\
			"AND B.product_id IN (9,10,1,2,3,7,8) " \
			"') "\
			"to 's3://verticals-raw-data/Ajani/Temp/latam/data_' "\
			"CREDENTIALS %s "\
			"ESCAPE "\
			"GZIP "\
			"ALLOWOVERWRITE "\
			"manifest;"
	return query

def getLoadDailyQuery():
	copy_query = "DELETE FROM verticals_bi.aux_latam_ppu WHERE time_id = '%s'; "\
					"COPY verticals_bi.aux_latam_ppu from 's3://verticals-raw-data/Ajani/Temp/latam/data_manifest' "\
					"CREDENTIALS %s "\
					"ESCAPE "\
					"GZIP "\
					"manifest;"
	return copy_query


latam_conf = sys.argv[1]
silver_conf = sys.argv[2]

yesterday_date = (date.today() - timedelta(1)).strftime('%Y/%m/%d')
print(yesterday_date)

deletePreviousS3Files(silver_conf)

# Get data from LATAM clusters
conn = getConnection(latam_conf)
cur = conn.cursor()
data = (getS3Keys(silver_conf),)
query = getUnloadQuery()
#data = (yesterday_date,yesterday_date,getS3Keys(silver_conf),)
#query = getUnloadDailyQuery()
cur.execute(query,data)
conn.commit()
cur.close()
conn.close()

#Load data into Silver
conn_silver = getConnection(silver_conf)
cur = conn_silver.cursor()
cred = (getS3Keys(silver_conf),)
copy_query = getLoadQuery()
#cred = (yesterday_date,getS3Keys(silver_conf),)
#copy_query = getLoadDailyQuery()
cur.execute(copy_query,cred)
conn_silver.commit()
cur.close()
conn_silver.close()
