import psycopg2
import simplejson as json
import sys


def getConnection(conf_file):
  	data = json.load(open(conf_file))	
  	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return 'aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s' \
	% {'key': data['s3_key'],'skey': data['s3_skey']}