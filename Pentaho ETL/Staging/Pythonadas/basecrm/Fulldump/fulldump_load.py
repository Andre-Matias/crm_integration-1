from load_resources import *
import sys

conf_file = sys.argv[1]
chandra_conf_file = sys.argv[2]

#Date of the fulldump yyyy/mm/dd format
fulldump_date = sys.argv[3]

##################################################
# Read conf_file
##################################################
file = open(conf_file, "r") 
temp = file.read().splitlines()
bucketName = temp[7]
path_fulldump = temp[9]
manifest = temp[11]
schema = temp[13]
platform = temp[15]
resources = temp[17].split(',')
file.close()

##################################################
# prefix parameter should be 'sync_' or ''
# Truncate tables before loading the fulldumps
##################################################
truncateResourceTables(chandra_conf_file,
	schema,
	resources,
	platform,
	'')

##################################################
# prefix parameter should be 'sync_' or ''
# Loads tables with fulldumps
##################################################
loadFromS3toRedshift(chandra_conf_file, 
	schema,
	platform,
	bucketName,
	path_fulldump,
	fulldump_date,
	manifest,
	resources,
	'')