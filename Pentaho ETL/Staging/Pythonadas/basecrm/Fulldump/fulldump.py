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
from fulldump_resources import *

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

s3_fulldump_deals(client,keyId,sKeyId)
