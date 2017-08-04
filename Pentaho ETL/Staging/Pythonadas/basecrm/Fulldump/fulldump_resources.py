import basecrm
import boto
from boto.s3.key import Key
from munch import *
import simplejson as json
from decimal import *
import os
import sys
import gzip
import dateutil.parser
from datetime import datetime

# Convert timestamps
def convert_timestamps(data):
	for item in data:
			for k in item:
				if(k[-3:] == '_at'):
					item[k] = str(dateutil.parser.parse(item[k]))[:-6]
	return data				


def s3_fulldump_deals(client,keyId,sKeyId,bucketName,path):
	
	#Iterate for everypage returned by the API
	aux = 1

	while 1:
		
		data = client.deals.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open("/home/ubuntu/Reports/deals_" + str(aux).zfill(10) + ".txt.gz", 'wb')

		data = convert_timestamps(data)

		#Iterate the list of deals
		for deal_data in data:
			output.write(json.dumps(deal_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
		localName = "/home/ubuntu/Reports/deals_" + str(aux).zfill(10) + ".txt.gz"
		fileName="deals_" + str(aux).zfill(10) + ".txt.gz"
		full_key_name = os.path.join(path+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1



def s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path):
	
	#Iterate for everypage returned by the API
	aux = 1

	while 1:
		
		data = client.contacts.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open("/home/ubuntu/Reports/contacts_" + str(aux).zfill(10) + ".txt.gz", 'wb')

		data = convert_timestamps(data)

		#Iterate the list of deals
		for contact_data in data:
			output.write(json.dumps(contact_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
		localName = "/home/ubuntu/Reports/contacts_" + str(aux).zfill(10) + ".txt.gz"
		fileName="contacts_" + str(aux).zfill(10) + ".txt.gz"
		full_key_name = os.path.join(path+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1
