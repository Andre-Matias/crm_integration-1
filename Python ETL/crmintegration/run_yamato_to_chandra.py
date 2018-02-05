import sys
import copy_tables_atlas
import copy_tables_hydra
import load_analytical_model
import scai
from datetime import date, datetime

COD_INTEGRATION = 10000		# Yamato to Chandra
COD_COUNTRY = 1				# Portugal

print(datetime.now().time())

conf_file = sys.argv[1] 		# File with names for the tables to copy
dml_file = sys.argv[2] 			# File with DML script for SCD2 tables
source_conf_file = sys.argv[3] 	# File with S3 path and source database
target_conf_file = sys.argv[4] 	# File with target database

scai.integrationStart(target_conf_file, COD_INTEGRATION, COD_COUNTRY) # SCAI

# Copy Atlas tables from Yamato to S3 to Chandra
copy_tables_atlas.main(conf_file, source_conf_file, target_conf_file)

# Copy Hydra tables from Yamato to S3 to Chandra (tables aren't fully copied here)
copy_tables_hydra.main(conf_file, source_conf_file, target_conf_file)

scai.integrationEnd(target_conf_file, COD_INTEGRATION, COD_COUNTRY) # SCAI
"""
print(datetime.now().time())
input("Everything copied from Yamato to Chandra! Proceed with running DML script?\n")
print(datetime.now().time())

# Load Chandra Analytical tables with data from Chandra Operational tables
load_analytical_model.main(target_conf_file, dml_file)
"""
print(datetime.now().time())
print('All done!')