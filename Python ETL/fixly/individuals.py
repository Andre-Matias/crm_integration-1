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
		# individuals table
		cur.execute('SELECT id, regexp_replace(  replace (replace( replace(details, \'\'\'\', \'\') ,\';\',\'-\'), \'"\', \'\' ), \'\r|\n\', \'\', \'g\') as details, settings, created_at, updated_at, deleted_at, \'\' as info, offer_quote_limit, regexp_replace(REPLACE(replace(company_name,\'\'\'\',\'\'), \'"\', \'\' ), \'\r|\n\', \'\', \'g\') as company_name, vat_number, id_number, wizard_step, user_id FROM individuals')

		
		# display the PostgreSQL database server version
		db_result = cur.fetchall()
		lista=db_result
		
		#the file thing
		thefile="individuals-data.txt"
		file = open(thefile, mode="w")
		for elemento in lista:
			#print(elemento) # Una simple verificación
			for line in elemento:
				line=str.replace(str(line), 'None', '')
				file.write(line)
				file.write(';')
			file.write('\n')
		file.close()
		
		#end the file thing
		f_in = open('individuals-data.txt', 'rb')
		f_out = gzip.open('individuals-data.txt.gz', 'wb')
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
		fileName="individuals-data.txt.gz"
		path = "/new/"
		full_key_name = os.path.join(path, fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name
		thefile2="individuals-data.txt.gz"
		k.set_contents_from_filename(thefile2)	
		#os.remove(thefile2)
		print("Move to S3")
	

#execute this	
connect()
sub_moveToS3 (keyId, sKeyId, bucketName)