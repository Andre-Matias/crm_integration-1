import sys
import simplejson as json
import copy_tables_atlas
import copy_tables_hydra
from datetime import date, datetime
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai

COD_INTEGRATION = 10000							# Yamato to Operational
COD_COUNTRY = -1								# Replaced by code in conf_file

print(datetime.now().time())

conf_file = sys.argv[1] 						# File with names for the tables to copy
db_conf_file = sys.argv[2] 						# File with source database
data = json.load(open(conf_file))

COD_COUNTRY = int(data['cod_country'])			# Global variable



country_execution_status = scai.getCountryIntegrationStatus(db_conf_file, COD_COUNTRY)	# SCAI

scai_last_execution_status = scai.getLastExecutionStatus(db_conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI


if (country_execution_status != 1 and scai_last_execution_status == 1):
	print ('The integration executed successfuly on last execution. The problem is further ahead.')
	sys.exit(0)

if (scai_last_execution_status == 2):
	sys.exit("The integration is already running...")

	
#Begin scai execution
scai.integrationStart(db_conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

# Copy CRM_Base tables from Yamato to Operational Model
scai_last_execution_status = copy_tables_crmbase.main(conf_file, db_conf_file, scai_last_execution_status)

# Copy Atlas tables from Yamato to Operational Model
scai_last_execution_status = copy_tables_atlas.main(conf_file, db_conf_file, scai_last_execution_status)

# Copy Hydra tables from Yamato to Operational Model (tables aren't fully copied here)
#scai_last_execution_status = copy_tables_hydra.main(conf_file, db_conf_file, scai_last_execution_status)

#End scai execution
scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 1)		# SCAI

print(datetime.now().time())
print('All done!')