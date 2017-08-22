from load_resources import *
import sys
import simplejson as json

conf_file = sys.argv[1]
chandra_conf_file = sys.argv[2]

#Date of the fulldump yyyy/mm/dd format
fulldump_date = sys.argv[3]

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
resources = data['resources'].split(',')

##################################################
# prefix parameter should be 'sync_' or ''
# Truncate tables before loading the fulldumps
##################################################
deleteCategoryCountryDataFromTables(chandra_conf_file,
	schema,
	resources,
	category,
	country,
	'')

##################################################
# prefix parameter should be 'sync_' or ''
# Loads tables with fulldumps
##################################################
loadFromS3toRedshift(chandra_conf_file, 
	schema,
	category,
	country,
	bucketName,
	path_fulldump,
	fulldump_date,
	manifest,
	resources,
	'')