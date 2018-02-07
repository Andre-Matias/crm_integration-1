import sys
import calculate_kpis
import copy_from_chandra_to_base
import scai
from datetime import date, datetime

COD_INTEGRATION = 50000		# Chandra to Base
COD_COUNTRY = 1				# Portugal

print(datetime.now().time())

conf_file = sys.argv[1] 		# File with names for the tables to copy
kpi_file = sys.argv[2] 			# File with DML script for SCD2 tables
source_conf_file = sys.argv[3] 	# File with S3 path and source database
target_conf_file = sys.argv[4] 	# File with target database

scai.integrationStart(target_conf_file, COD_INTEGRATION, COD_COUNTRY) # SCAI

# Calculate KPIs with data from Chandra
calculate_kpis.main(conf_file, kpi_file)

# Copy Hydra tables from Yamato to S3 to Chandra (tables aren't fully copied here)
copy_from_chandra_to_base.main(conf_file, source_conf_file, target_conf_file)

scai.integrationEnd(target_conf_file, COD_INTEGRATION, COD_COUNTRY) # SCAI

print(datetime.now().time())
print('All done!')