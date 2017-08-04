import basecrm
import os
import sys
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

#Configure Base API client
client = basecrm.Client(access_token=access_token_base)

# Full dumps
s3_fulldump_deals(client,keyId,sKeyId,bucketName,path)
s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path)