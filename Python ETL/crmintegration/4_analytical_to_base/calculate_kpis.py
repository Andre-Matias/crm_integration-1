import sys, os
from datetime import datetime
import psycopg2
import simplejson as json
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai

COD_INTEGRATION = 50000		# Analytical to Base

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def main(db_conf_file, kpi_file):
	
	print('Connecting to Database...')
	print(datetime.now().time())
	
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()
	kpi_scripts = open(kpi_file).read().split('$$$')

	#print('Scripts: ' + kpi_scripts)
	print('Executing KPI scripts...')
	print(datetime.now().time())

	i = 1
	for kpi in kpi_scripts:
		#if i < 16:  # Remove comments to make this run starting from a certain block
		#	i = i + 1
		#	continue
		print('Running block #' + str(i))
		print(datetime.now().time())
		cur.execute(kpi)
		conn.commit()
		i = i + 1

	print('Closing Database connection...')
	print(datetime.now().time())

	cur.close()
	conn.close()
	

# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	db_conf_file = sys.argv[1] 	# Database configuration file
	kpi_file = sys.argv[2]  	# File with KPI calculation scripts to execute
	
	main(db_conf_file, kpi_file)