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
	conn = S3Connection()
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
		"IAM_ROLE '%(credentials)s';" \
		% {
		'schema': schema,
		'table': table,
		'bucket': bucket,
		'manifest': manifest,
		'credentials': credentials
	}

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def getIAMRole(conf_file):
	data = json.load(open(conf_file))
	return data['iam_role']
	#return "aws_iam_role=" + data['iam_role']
	
def deletePreviousS3Files(bucketName, data_path):
	print("Deleting S3 Imopt Contacts Files")

	conn = S3Connection()
	b = Bucket(conn, bucket_name)
	for x in b.list(prefix = data_path + 'contacts/imopt/'):
		x.delete()

@retry(exceptions=Exception, delay=1, tries=10, logger=logger)			
def s3_fulldump_contacts(client,bucketName,data_path,category,country):
	
	print("Getting contacts data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "./aut_imopt_base_to_bd_contact_"
	while 1:
		
		data = client.contacts.list(page = aux, per_page = 100)
		
		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')
		
		#Iterate the list of contacts
		for contact_data in data:
			if str(datetime.strptime(contact_data.updated_at[:10], '%Y-%m-%d')) >= str(datetime.today().date() - timedelta(days=7)):
				contact = Munch()
				contact.id = contact_data.id
				contact.fiscal_address = contact_data.address
				contact.email_address = contact_data.email
				if 'NIF' in contact_data.custom_fields:
					contact.nif = contact_data.custom_fields['NIF']
				else:
					contact.nif = ''
				if 'User_ID' in contact_data.custom_fields:
					contact.user_id = contact_data.custom_fields['User_ID']
				else:
					contact.user_id = ''
				if 'Designação Fiscal' in contact_data.custom_fields:
					contact.designacao_fiscal = contact_data.custom_fields['Designação Fiscal']
				else:
					contact.designacao_fiscal = ''
				if 'Natureza' in contact_data.custom_fields:
					contact.natureza = contact_data.custom_fields['Natureza']
				else:
					contact.natureza = ''
				contact.country = country
				contact.category = category
				output.write((json.dumps(contact,use_decimal=True)+"\n").encode('utf-8'))

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="aut_imopt_base_to_bd_contact_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(data_path+"contacts/imopt/", fileName)
		conn = boto.connect_s3()
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1
			
def loadFromS3toRedshift(conf_file,schema,category,country,bucketName,data_path,date,manifest_path):
	conn = getDatabaseConnection(conf_file)
	credentials = getAIMRole(conf_file)
	cur = conn.cursor()
	
	if(checkS3FileExists(conf_file,bucketName,str(data_path) + 'contacts/imopt/') == 'true'):
		print('Loading...')
		cur.execute(
			getCopySql(
				schema, \
				'aut_imopt_base_to_bd_contact',
				's3://%(bucketName)s%(data_path)scontacts/imopt/' \
					% {
					'bucketName':bucketName,
					'date': date,
					'data_path':data_path},
				's3://%(bucketName)s%(manifest_path)saut_imopt_contacts_sync_jsonpath.json' \
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
	base_api_token = json.load(open(conf_file))['base_api_token_imopt'] 
	schema = json.load(open(conf_file))['schema']
	country = json.load(open(conf_file))['country_pt']
	category = json.load(open(conf_file))['category_imopt']
	data_path = json.load(open(conf_file))['data_path']
	bucketName = json.load(open(conf_file))['bucketName']
	manifest_path = json.load(open(conf_file))['manifest_path']
	
	client = basecrm.Client(access_token=base_api_token)
	date = str(datetime.now().strftime('%Y/%m/%d/'))
	
### TODO - DELETE S3 PATH BEFORE UNLOADING!!!!
	deletePreviousS3Files(bucketName, data_path)
	
	s3_fulldump_contacts(client,bucketName,data_path,category,country)

	loadFromS3toRedshift(conf_file,schema,category,country,bucketName,data_path,date,manifest_path)
	
if __name__ == "__main__":
	conf_file = sys.argv[1] # File with source database
	main(conf_file)