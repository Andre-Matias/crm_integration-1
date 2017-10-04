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
		text_file = open(workspace + jql_scripts[context], "r")
		
		jqlquery = text_file.read().encode('utf-8')
		jqlquery = jqlquery.replace('FROM_DATE_REPLACE',from_date)
		jqlquery = jqlquery.replace('TO_DATE_REPLACE',to_date)

		print jqlquery
		text_file.close()
		data = getMixpanelDataAPI(token,jqlquery)
		#Add additional metadata columns
		for i in data:
			i['project_name']=project_name

		output = gzip.open(str(context) + ".txt.gz", 'wb')
		output.write(json.dumps(data,use_decimal=True)+"\n")
		output.close()

		sendToS3("verticals-raw-data","/vas/mixpanel/",context,workspace,project_name,keyId,skeyId,from_date)

		os.remove(workspace + str(context) + ".txt.gz")


mixpanel_conf = sys.argv[2]
project_name = sys.argv[3]
conf = json.load(open(mixpanel_conf))
token = conf[project_name]

script = [
	'/Users/miguelchin/Repos/verticals-bi/Python ETL/vas/jql/impressions.sql',
	'/Users/miguelchin/Repos/verticals-bi/Python ETL/vas/jql/loads.sql',
	'/Users/miguelchin/Repos/verticals-bi/Python ETL/vas/jql/leads.sql'
]
contexts = ["impressions","loads","leads"]
workspace = "/home/ubuntu/github-etl/Python ETL/vas/temp/"


conf_file = sys.argv[1]
conf = json.load(open(conf_file))
key = conf['s3_key']
skey = conf['s3_skey']


try:
	from_date = sys.argv[4]
except IndexError:
	from_date = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

split_date = from_date.split('-')
to_date = from_date

jql_scripts = {}
for i in range(len(contexts)):
    jql_scripts[contexts[i]] = script[i]

getMixpanelData(contexts,jql_scripts,workspace,project_name,key,skey,from_date,to_date)



