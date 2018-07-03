import sys, os
import simplejson as json
import calculate_kpis
import copy_from_analytical_to_base
from datetime import date, datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai

COD_INTEGRATION = 50000					# Analytical to Base
COD_COUNTRY = -1						# Replaced by code in conf_file

print(datetime.now().time())

db_conf_file = sys.argv[1]				# File with source database
conf_files = []							# Array with configuration files for each Base instance
kpi_files = []							# Array with KPIs files for each Base instance

i = 2
while i < len(sys.argv):
	conf_files.append(sys.argv[i])
	kpi_files.append(sys.argv[i+1])
	i += 2

COD_COUNTRY = int(json.load(open(conf_files[0]))['cod_country'])	# Global variable; all configuration files should have the same country code, so we only get the first

country_execution_status = scai.getCountryIntegrationStatus(db_conf_file, COD_COUNTRY)	# SCAI

scai_last_execution_status = scai.getLastExecutionStatus(db_conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

if (country_execution_status != 1 and scai_last_execution_status == 1):
	print ('The integration executed successfuly on last execution. The problem is further ahead.')
	sys.exit(0)

if (scai_last_execution_status == 2):
	sys.exit("The integration is already running...")
	
scai.integrationStart(db_conf_file, COD_INTEGRATION, COD_COUNTRY) 	# SCAI

# Calculate KPIs with data from the Analytical Model
for i in range(0, len(conf_files)):
	print('Calculating KPIs in file ' + kpi_files[i] + ' using configuration file ' + conf_files[i] + '...')
	calculate_kpis.main(db_conf_file, kpi_files[i], COD_COUNTRY)
	#input('Ready for next set of KPIs?')

# Send all KPIs to Base
for i in range(0, len(kpi_files)):
	print('Sending KPIs to Base using configuration file ' + conf_files[i] + '...')
	copy_from_analytical_to_base.main(db_conf_file, conf_files[i])

scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 1) 	# SCAI

print(datetime.now().time())
print('All done!')