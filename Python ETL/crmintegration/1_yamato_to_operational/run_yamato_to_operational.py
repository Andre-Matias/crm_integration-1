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

scai.integrationStart(db_conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

# Copy Atlas tables from Yamato to Operational Model
#copy_tables_atlas.main(conf_file, db_conf_file)

#input('Ready to copy Hydra tables. Proceed?')

# Copy Hydra tables from Yamato to Operational Model (tables aren't fully copied here)
copy_tables_hydra.main(conf_file, db_conf_file)

scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY)		# SCAI

print(datetime.now().time())
print('All done!')