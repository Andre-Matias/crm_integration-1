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

conf_file = sys.argv[1]

#Get access token and keys
file = open(conf_file, "r") 
temp = file.read().splitlines()
access_token_base = temp[1]
keyId = temp[3]
sKeyId = temp[5]
bucketName = temp[7]
path = temp[9]
file.close()

# Convert timestamps
def convert_timestamps(data):
	for item in data:
			for k in item:
				if(k[-3:] == '_at'):
					item[k] = str(dateutil.parser.parse(item[k]))[:-6]
	return data				

#Configure Base API client
client = basecrm.Client(access_token=access_token_base)

#Iterate for everypage returned by the API
aux = 1

while 1:

	print("Downloading page #" + str(aux))
	
	data = client.deals.list(page = aux, per_page = 100)

	if len(data) > 0: empty = False
	else: break

	#Write on local gz file
	print("Writing file #" + str(aux))	
	output = gzip.open("/home/ubuntu/Reports/latam_deals_" + str(aux).zfill(10) + ".txt.gz", 'wb')

	data = convert_timestamps(data)

	#Iterate the list of deals
	for deal_data in data:
		output.write(json.dumps(deal_data,use_decimal=True)+"\n")

	#Close gz file		
	output.close()

	#Upload file to S3
	print("Uploading to S3")
	localName = "/home/ubuntu/Reports/latam_deals_" + str(aux).zfill(10) + ".txt.gz"
	fileName="latam_deals_" + str(aux).zfill(10) + ".txt.gz"
	full_key_name = os.path.join(path, fileName)
	conn = boto.connect_s3(keyId,sKeyId)
	bucket = conn.get_bucket(bucketName)
	k = bucket.new_key(full_key_name)
	k.key=full_key_name

	k.set_contents_from_filename(localName)
	
	#Remove local gz file
	os.remove(localName)
	
	#Next page iterate
	aux += 1

