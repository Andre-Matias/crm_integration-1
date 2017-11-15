import simplejson as json 
import requests
import gzip
import boto
from boto.s3.key import Key
import os
import sys
from datetime import date, timedelta
import psycopg2
from boto.s3.connection import S3Connection, Bucket, Key


def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def getConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])


def deletePreviousS3Files(conf_file):
	conf = json.load(open(conf_file))
	key = conf['s3_key']
	skey = conf['s3_skey']

	conn = S3Connection(key, skey)
	b = Bucket(conn, 'verticals-raw-data')
	for x in b.list(prefix = 'vas/silver'):
		x.delete()


def unloadDataToS3(silver_conf):

	credentials = getS3Keys(silver_conf)

	query = "UNLOAD ('select * from livesync.verticals_ads') \
		to 's3://verticals-raw-data/yamato/silver/data_' \
		CREDENTIALS %(credentials)s \
		ESCAPE \
		GZIP \
		ALLOWOVERWRITE \
		manifest;"

	conn = getConnection(silver_conf)
	cur = conn.cursor()

	data = {'credentials': credentials}
	cur.execute(query,data)
	conn.commit()
	cur.close()
	conn.close()

def loadDataToRedshift(conf_file):

	credentials = getS3Keys(conf_file)

	query = "TRUNCATE TABLE livesync.verticals_ads2; \
					COPY livesync.verticals_ads2 from 's3://verticals-raw-data/yamato/silver/data_manifest' \
					CREDENTIALS %(credentials)s \
					ESCAPE \
					GZIP \
					manifest;"

	conn = getConnection(conf_file)
	cur = conn.cursor()

	data = {'credentials': credentials}
	cur.execute(query,data)
	conn.commit()
	cur.close()
	conn.close()


conf_file = sys.argv[1]
silver_file = sys.argv[2]

##deletePreviousS3Files(silver_file)
unloadDataToS3(silver_file)
loadDataToRedshift(conf_file)
