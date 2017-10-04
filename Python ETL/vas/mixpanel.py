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


def getChandraConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

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

def getMixpanelDataAPI(token,script):
	url = "https://mixpanel.com/api/2.0/jql"
	response = requests.post(url,
		headers={'Authorization':'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(token))},
		params={'script':script}
		)
				
	return response.json()

def sendToS3(bucketName,path,context,workspace,project_name,keyId,skeyId,date):

	localname = workspace + str(context) + ".txt.gz"
	full_key_name = os.path.join(path + project_name + str("/") + date.replace('-','/') + str("/") ,str(context) + ".txt.gz")

	conn = boto.connect_s3(keyId,skeyId)
	bucket = conn.get_bucket(bucketName)
	k = bucket.new_key(full_key_name)
	k.key=full_key_name

	k.set_contents_from_filename(localname)

def getMixpanelData(contexts,jql_scripts,workspace,project_name,keyId,skeyId,from_date,to_date):
	for context in contexts:
		print(jql_scripts)
		text_file = open(jql_scripts[context], "r")
		
		jqlquery = text_file.read().encode('utf-8')
		jqlquery = jqlquery.replace('FROM_DATE_REPLACE',from_date)
		jqlquery = jqlquery.replace('TO_DATE_REPLACE',to_date)

		print jqlquery
		text_file.close()
		data = getMixpanelDataAPI(token,jqlquery)
		#Add additional metadata columns
		for i in data:
			i['project_name']=project_name

		output = gzip.open(workspace + str(context) + ".txt.gz", 'wb')
		output.write(json.dumps(data,use_decimal=True)+"\n")
		output.close()

		sendToS3("verticals-raw-data","/vas/mixpanel/",context,workspace,project_name,keyId,skeyId,from_date)

		os.remove(workspace + str(context) + ".txt.gz")

def loadFilesToRedshift(conf_file,bucket,data_path,contexts,date,manifest_path):
	conn = getChandraConnection(conf_file)
	credentials = getS3Keys(conf_file)

	date = date.replace('-','/')
	cur = conn.cursor()
	
	for context in contexts:
		cur.execute(
			getCopySql(
				"vas",
				'mixpanel_%(resource)s' \
								% {
								'resource':context},
				's3://%(bucket)s%(data_path)s/%(date)s/%(resource)s.txt.gz' \
								% {
								'resource':context,
								'bucket':bucket,
								'date': date,
								'data_path':data_path},
				's3://%(bucket)s%(manifest_path)s%(resource)s_jsonpath.json' \
								% {
								'resource':context,
								'bucket':bucket,
								'manifest_path':manifest_path
								}, 
				credentials)
			)
	conn.commit()


	cur.close()
	conn.close()

mixpanel_conf = sys.argv[2]
project_name = sys.argv[3]
conf = json.load(open(mixpanel_conf))
token = conf[project_name]


if(sys.argv[4] == 'cars'):
	contexts = ["impressions","loads","leads"]
	script = [
	'/home/ubuntu/github-etl/Python ETL/vas/jql/impressions.sql',
	'/home/ubuntu/github-etl/Python ETL/vas/jql/loads.sql',
	'/home/ubuntu/github-etl/Python ETL/vas/jql/leads.sql']

if(sys.argv[4] == 're'):
	contexts = ["impressions_re","loads_re","leads_re"]
	script = [
	'/home/ubuntu/github-etl/Python ETL/vas/jql/impressions_re.sql',
	'/home/ubuntu/github-etl/Python ETL/vas/jql/loads_re.sql',
	'/home/ubuntu/github-etl/Python ETL/vas/jql/leads_re.sql']

workspace = "/home/ubuntu/github-etl/Python ETL/vas/"


conf_file = sys.argv[1]
conf = json.load(open(conf_file))
key = conf['s3_key']
skey = conf['s3_skey']


try:
	from_date = sys.argv[5]
except IndexError:
	from_date = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

split_date = from_date.split('-')
to_date = from_date

jql_scripts = {}
for i in range(len(contexts)):
    jql_scripts[contexts[i]] = script[i]

#getMixpanelData(contexts,jql_scripts,workspace,project_name,key,skey,from_date,to_date)

loadFilesToRedshift(conf_file,"verticals-raw-data","/vas/mixpanel/" + project_name,contexts,to_date,"/vas/mixpanel/manifests/")

