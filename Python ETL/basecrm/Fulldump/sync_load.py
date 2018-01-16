from load_resources import *
import sys
import simplejson as json
from datetime import date

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
redshift_data = json.load(open(target_conf_file))

bucketName = data['bucket_name']
path_fulldump = data['s3_data_path_sync']
manifest = data['s3_manifest_path']
schema = redshift_data['redshift_schema']
category = data['category']
country = data['country']
resources = data['resources_sync'].split(',')
prefix = 'sync_'

##################################################
# prefix parameter should be 'sync_' or ''
# Truncate tables before loading the syncs
##################################################
truncateResourceTables(target_conf_file,
	schema,
	resources,
	category,
	country,
	prefix)

##################################################
# prefix parameter should be 'sync_' or ''
# Loads tables with syncs
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

##################################################
# Updates the main tables using the sync tables
##################################################
syncDealsTable(target_conf_file,schema,category,country)
syncContactsTable(target_conf_file,schema,category,country)
syncLeadsTable(target_conf_file,schema,category,country)
syncUsersTable(target_conf_file,schema,category,country)
syncCallsTable(target_conf_file,schema,category,country)
syncTagsTable(target_conf_file,schema,category,country)
syncOrdersTable(target_conf_file,schema,category,country)
syncLineItemsTable(target_conf_file,schema,category,country)
#syncTasksTable(target_conf_file,schema,category,country)






