import psycopg2
import numpy as np
import time
import datetime
import simplejson as json

def getCopySql(schema, table, bucket, manifest, credentials):
    return "COPY %(schema)s.%(table)s\n" \
		"FROM '%(bucket)s'\n" \
		"JSON AS '%(manifest)s'\n" \
		"dateformat 'auto'\n" \
		"timeformat 'YYYY-MM-DDTHH:MI:SS'\n" \
		"gzip\n" \
		"CREDENTIALS '%(credentials)s';" \
		% {
		'schema': schema,
		'table': table,
		'bucket': bucket,
		'manifest': manifest,
		'credentials': credentials
	}

def getChandraConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def loadFromS3toRedshift(conf_file,schema,category,country,bucket,data_path,date,manifest_path,resources,prefix):
	conn = getChandraConnection(conf_file)
	credentials = getS3Keys(conf_file)

	cur = conn.cursor()

	for resource in resources:
		print(resource)
		cur.execute(
			getCopySql(
				schema, \
				'%(prefix)sstg_d_base_%(resource)s' \
					% {
					'resource':resource,
					'category':category,
					'country':country,
					 'prefix': prefix},
				's3://%(bucket)s%(data_path)s%(resource)s/%(date)s/' \
					% {
					'resource':resource,
					'bucket':bucket,
					'date': date,
					'data_path':data_path},
				's3://%(bucket)s%(manifest_path)s%(prefix)s%(resource)s_jsonpath.json' \
					% {
					'prefix': prefix,
					'resource':resource,
					'bucket':bucket,
					'manifest_path':manifest_path
					}, 
				credentials
			)
		)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def truncateResourceTables(conf_file,schema,resources,category,country,prefix):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	for resource in resources:
		cur.execute("TRUNCATE TABLE %(schema)s.%(prefix)sstg_d_base_%(resource)s" \
			% {
			'resource':resource,
			'category':category,
			'country':country,
			'prefix': prefix,
			'schema': schema
			}
		)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()


def deleteCategoryCountryDataFromTables(conf_file,schema,resources,category,country,prefix):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	for resource in resources:
		cur.execute("DELETE FROM %(schema)s.%(prefix)sstg_d_base_%(resource)s" \
			" WHERE base_account_country = '%(country)s'" \
			" AND base_account_category = '%(category)s'"  
			% {
			'resource':resource,
			'category':category,
			'country':country,
			'prefix': prefix,
			'schema': schema,
			'country':country,
			'category':category
			}
		)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()	

