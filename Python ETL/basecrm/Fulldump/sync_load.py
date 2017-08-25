from load_resources import *
import sys
from datetime import date

conf_file = sys.argv[1]
chandra_conf_file = sys.argv[2]

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
path_fulldump = data['s3_data_path_sync']
manifest = data['s3_manifest_path']
schema = data['redshift_schema']
category = data['category']
country = data['country']
resources = data['resources_sync'].split(',')
prefix = 'sync_'

##################################################
# prefix parameter should be 'sync_' or ''
# Truncate tables before loading the syncs
##################################################
truncateResourceTables(chandra_conf_file,
	schema,
	resources,
	category,
	country,
	prefix)

##################################################
# prefix parameter should be 'sync_' or ''
# Loads tables with syncs
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
	prefix)

syncDealsTable(chandra_conf_file,schema,category,country)
#Calls, Contacts, Deals, Leads, Line Items, Orders, Products***, Tags, Users






