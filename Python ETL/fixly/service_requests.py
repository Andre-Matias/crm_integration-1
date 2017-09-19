#!/usr/bin/python

#- * -coding: utf - 8 - * -
import gzip
import psycopg2
from config import config
from string import Template
import codecs
import os
import boto
from boto.s3.key import Key
import simplejson as json

#pip install unicodedata2


bucketName="vertical-services-poland-2"
data = json.load(open('/home/ubuntu/conf/chandra.conf'))
keyId = data['s3_key']
sKeyId = data['s3_skey']

def connect():
	conn = None
	try:
		params = config()
		print('Connecting to the PostgreSQL database...')
		conn = psycopg2.connect(**params)
		# create a cursor
		cur = conn.cursor()
		# execute a statement
		print('PostgreSQL database version:')
		
		#QUERY 1
		# service_requests table
		cur.execute('SELECT CAST(id as integer) as id, CAST(user_id as integer) as user_id, CAST(category_id as integer) as category_id, status , CAST(to_user_id as integer) as to_user_id, CAST(lat as DECIMAL(10,4)) as lat, CAST(lon as DECIMAL(10,4)) as lon  , regexp_replace(address, \'\r|\n\', \'\', \'g\') as address, user_agent, CAST(created_at as TIMESTAMP) AS created_at, CAST(updated_at as TIMESTAMP) AS updated_at , CAST(deleted_at as TIMESTAMP) AS deleted_at  , null as exactdate , CAST(city_id as integer) as city_id FROM service_requests')

		# display the PostgreSQL database server version
		db_result = cur.fetchall()
		lista=db_result
		
		#the file thing
		thefile="service_requests-data.txt"
		file = open(thefile, mode="w", encoding='utf-8')
		for elemento in lista:
			#print(elemento) # Una simple verificación
			for line in elemento:
				line=str.replace(str(line), 'None', '')
				file.write(line)
				file.write(';')
			file.write('\n')
		file.close()
		
		#end the file thing
		f_in = open('service_requests-data.txt', 'rb')
		f_out = gzip.open('service_requests-data.txt.gz', 'wb')
		f_out.writelines(f_in)
		f_out.close()
		f_in.close()
		
		#delete the txt
		os.remove(thefile)
		
		#close connectiondb
		cur.close()

	except(Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
		  conn.close()
		print('Database connection closed.')

def sub_moveToS3 (keyId, sKeyId, bucketName):
		fileName="service_requests-data.txt.gz"
		path = "/new/"
		full_key_name = os.path.join(path, fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name
		thefile2="service_requests-data.txt.gz"
		k.set_contents_from_filename(thefile2)	
		#os.remove(thefile2)
		print("Move to S3")
	

#execute this	
connect()
sub_moveToS3 (keyId, sKeyId, bucketName)