#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import basecrm
import boto
from boto.s3.key import Key
from munch import *
import simplejson as json
from decimal import *
import os
import sys
import gzip
from datetime import datetime, timedelta
import requests
import time
import psycopg2
import numpy as np
from boto.s3.connection import S3Connection, Bucket, Key
from retry import retry
import logging

logging.basicConfig()
logger = logging.getLogger('logger')

def checkS3FileExists(conf_file,bucket,path):
	conf = json.load(open(conf_file))
	key = conf['s3_key']
	skey = conf['s3_skey']
	conn = S3Connection(key, skey)
	b = Bucket(conn, bucket)
	found_file = 'false'

	for x in b.list(prefix = path[1:]):
		if(len(str(x)) > 0):
			print(path)
			found_file = 'true'
			break

	return found_file

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

def deletePreviousS3Files(conf_file, keyId, sKeyId):
	print("Deleting S3 Olxpt Deals Files")
	conf = json.load(open(conf_file))

	conn = S3Connection(keyId, sKeyId)
	b = Bucket(conn, 'pyrates-data-ocean')
	for x in b.list(prefix = 'renato-teixeira/deals/olxpt/'):
		x.delete()

@retry(exceptions=Exception, delay=1, tries=10, logger=logger)			
def s3_fulldump_deals(client,keyId,sKeyId,bucketName,data_path,category,country):
	
	print("Getting deals data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "./aut_olxpt_base_to_bd_deal_"
	while 1:
		
		data = client.deals.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for deal_data in data:
			#if  str(datetime.strptime(deal_data.updated_at[:10], '%Y-%m-%d')) >= str(datetime.today().date() - timedelta(days=7)):
			deal = Munch()
			deal.id = deal_data.id
			deal.contact_id = deal_data.contact_id
			if 'Categoria' in deal_data.custom_fields:
				deal.categoria = deal_data.custom_fields['Categoria']
			else:
				deal.categoria = ''
			if 'Email' in deal_data.custom_fields:
				deal.email = deal_data.custom_fields['Email']
			else:
				deal.email = ''
			if 'Mobile' in deal_data.custom_fields:
				deal.mobile = deal_data.custom_fields['Mobile']
			else:
				deal.mobile = ''
			if 'NIF' in deal_data.custom_fields:
				deal.nif = deal_data.custom_fields['NIF']
			else:
				deal.nif = ''
			if 'Phone' in deal_data.custom_fields:
				deal.phone = deal_data.custom_fields['Phone']
			else:
				deal.phone = ''
			if 'User ID' in deal_data.custom_fields:
				deal.user_id = deal_data.custom_fields['User ID']
			else:
				deal.user_id = ''
			deal.country = country
			deal.category = category
			output.write((json.dumps(deal,use_decimal=True)+"\n").encode('utf-8'))

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="aut_olxpt_base_to_bd_deal_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(data_path+"deals/olxpt/", fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1
			
def loadFromS3toRedshift(conf_file,schema,category,country,bucketName,data_path,date,manifest_path):
	conn = getChandraConnection(conf_file)
	credentials = getS3Keys(conf_file)
	cur = conn.cursor()
	
	if(checkS3FileExists(conf_file,bucketName,str(data_path) + 'deals/olxpt/') == 'true'):
		print('Loading...')
		cur.execute(
			getCopySql(
				schema, \
				'aut_olxpt_base_to_bd_deal', #'aux_olxpt_deals',
				's3://%(bucketName)s%(data_path)sdeals/olxpt/' \
					% {
					'bucketName':bucketName,
					'date': date,
					'data_path':data_path},
				's3://%(bucketName)s%(manifest_path)saut_olxpt_deals_sync_jsonpath.json' \
					% {
					'bucketName':bucketName,
					'manifest_path':manifest_path
					}, 
				credentials
			)
		)
		conn.commit()

	#Close connection
	cur.close()
	conn.close()

def main(conf_file):
	base_api_token = json.load(open(conf_file))['base_api_token_olxpt']
	schema = json.load(open(conf_file))['schema']
	country = json.load(open(conf_file))['country_pt']
	category = json.load(open(conf_file))['category_olxpt']
	data_path = json.load(open(conf_file))['data_path']
	keyId = json.load(open(conf_file))['s3_key']
	sKeyId = json.load(open(conf_file))['s3_skey']
	bucketName = json.load(open(conf_file))['bucketName']
	manifest_path = json.load(open(conf_file))['manifest_path']
		
	client = basecrm.Client(access_token=base_api_token)
	date = str(datetime.now().strftime('%Y/%m/%d/'))
	
### TODO - DELETE S3 PATH BEFORE UNLOADING!!!!
	deletePreviousS3Files(conf_file, keyId, sKeyId)
	
	s3_fulldump_deals(client,keyId,sKeyId,bucketName,data_path,category,country)

	loadFromS3toRedshift(conf_file,schema,category,country,bucketName,data_path,date,manifest_path)
	
if __name__ == "__main__":
	conf_file = sys.argv[1] # File with source database
	main(conf_file)