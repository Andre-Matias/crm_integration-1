import sys
import simplejson as json
import copy_tables_basecrm
from datetime import date, datetime
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))  # Change this later to a package import
import scai


COD_INTEGRATION = 11000					# Chandra to Operational
COD_COUNTRY = -1						# Replaced by code in conf_file


print(datetime.now().time())

conf_file = sys.argv[1] 				# File with names for the tables to copy and S3 path
source_conf_file = sys.argv[2] 			# File with source database
target_conf_file = sys.argv[3] 			# File with target database
data = json.load(open(conf_file))

COD_COUNTRY = int(data['cod_country'])	# Global variable

scai.integrationStart(target_conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

# Copy rdl_basecrm_v2 tables from Chandra to Operational Model
copy_tables_basecrm.main(conf_file, source_conf_file, target_conf_file)

scai.integrationEnd(target_conf_file, COD_INTEGRATION, COD_COUNTRY)		# SCAI

print(datetime.now().time())
print('All done!')