import psycopg2
import simplejson as json
import sys

def getConnection(conf_file):
  	data = json.load(open(conf_file))	
  	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def getUnloadQuery(i):
	query = "UNLOAD ('select * from verticals_bi.dim_geography_l%s') \
		to 's3://verticals-raw-data/Ajani/Temp/geo%s/data_' \
		CREDENTIALS %s \
		ESCAPE \
		GZIP \
		ALLOWOVERWRITE \
		manifest;"
	return query

def getLoadQuery(i):
	copy_query = "TRUNCATE TABLE ajani.dim_geography_l%s; \
					COPY ajani.dim_geography_l%s from 's3://verticals-raw-data/Ajani/Temp/geo%s/data_manifest' \
					CREDENTIALS %s \
					ESCAPE \
					GZIP \
					manifest;"
	return copy_query


silver_conf = sys.argv[1]
chandra_conf = sys.argv[2]

credentials = getS3Keys(chandra_conf)

for i in range(1,4):
	conn = getConnection(silver_conf)
	cur = conn.cursor()

	query = getUnloadQuery(i)
	cred = (i,i,credentials)
	cur.execute(query,cred)
	conn.commit()
	cur.close()
	conn.close()

	conn_chandra = getConnection(chandra_conf)
	cur = conn_chandra.cursor()


	copy_query = getLoadQuery(i)
	cred = (i,i,i,credentials)
	cur.execute(copy_query,cred)
	conn_chandra.commit()
	cur.close()
	conn_chandra.close()

