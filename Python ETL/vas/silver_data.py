import simplejson as json 
import requests
import base64
import urllib # for url encoding
import gzip
import boto
from boto.s3.key import Key
import os
import sys
from datetime import date, timedelta
import psycopg2


def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def getConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])


def unloadDataToS3(silver_conf):

	credentials = getS3Keys(silver_conf)

	query = "UNLOAD ('select * from livesync.verticals_ads') \
		to 's3://verticals-raw-data/vas/silver/data_' \
		CREDENTIALS %(credentials)s \
		ESCAPE \
		GZIP \
		ALLOWOVERWRITE \
		manifest;"

	conn = getConnection(silver_conf)
	cur = conn.cursor()

	data = {'date': credentials}
	cur.execute(query,data)
	conn.commit()
	cur.close()
	conn.close()

def loadDataToRedshift(conf_file):

	credentials = getS3Keys(conf_file)

	query = "TRUNCATE TABLE vas.verticals_ads; \
					COPY vas.verticals_ads from 's3://verticals-raw-data/vas/silver/data_manifest' \
					CREDENTIALS %(credentials)s \
					ESCAPE \
					GZIP \
					manifest;"

	conn = getConnection(conf_file)
	cur = conn.cursor()

	data = {'date': credentials}
	cur.execute(query,data)
	conn.commit()
	cur.close()
	conn.close()


conf_file = sys.argv[1]
silver_file = sys.argv[2]

unloadDataToS3(silver_file)
loadDataToRedshift(conf_file)
