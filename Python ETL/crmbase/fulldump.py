import basecrm
import os
import sys
from fulldump_resources import *

conf_file = sys.argv[1]

##################################################
#Get access token and other data
##################################################
data = json.load(open(conf_file))

access_token_base = data['base_api_token']
bucketName = data['bucket_name']
path = data['s3_data_path']
category = data['category']
country = data['country']
resources = data['resources'].split(',')

##################################################
#Configure Base API client
##################################################
client = basecrm.Client(access_token=access_token_base)

##################################################
# Full dumps
##################################################
for resource in resources:
	mapping_fulldump_methods(resource,access_token_base,bucketName,path,client,country,category)
