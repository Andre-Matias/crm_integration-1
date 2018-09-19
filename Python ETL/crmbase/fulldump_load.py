from load_resources import *
import sys
import simplejson as json
from datetime import date, datetime

print(datetime.now().time())
conf_file = sys.argv[1]
target_conf_file = sys.argv[2]

#Date of the fulldump yyyy/mm/dd format

try:
	fulldump_date = sys.argv[3]
except IndexError:
	fulldump_date = str(date.today().strftime('%Y/%m/%d'))

##################################################
# Read conf_file
##################################################
data = json.load(open(conf_file))

bucketName = data['bucket_name']
path_fulldump = data['s3_data_path']
manifest = data['s3_manifest_path']
schema = data['redshift_schema']
category = data['category']
country = data['country']
resources = data['resources_fulldump_load'].split(',')
prefix = ''

##################################################
# prefix parameter should be 'sync_' or ''
# Truncate tables before loading the fulldumps
##################################################
deleteCategoryCountryDataFromTables(target_conf_file,
	schema,
	resources,
	category,
	country,
	prefix)

##################################################
# prefix parameter should be 'sync_' or ''
# Loads tables with fulldumps
##################################################
loadFromS3toRedshift(target_conf_file, 
	schema,
	category,
	country,
	bucketName,
	path_fulldump,
	fulldump_date,
	manifest,
	resources,
	prefix)

if 'deals' in resources:
	copyDumpToHistoryTable(target_conf_file,schema,category,country)
	
print(datetime.now().time())
