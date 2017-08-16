import basecrm
import os
import sys
from fulldump_resources import *

conf_file = sys.argv[1]

##################################################
#Get access token and keys
##################################################
file = open(conf_file, "r") 
temp = file.read().splitlines()
access_token_base = temp[1]
keyId = temp[3]
sKeyId = temp[5]
bucketName = temp[7]
path = temp[9]
category = temp[15]
country = temp[17]
resources = temp[19].split(',')
file.close()

##################################################
#Configure Base API client
##################################################
client = basecrm.Client(access_token=access_token_base)

##################################################
# Full dumps
##################################################
for resource in resources:
	mapping_fulldump_methods(resource,access_token_base,keyId,sKeyId,bucketName,path,client,category,country)






