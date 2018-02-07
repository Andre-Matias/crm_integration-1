import sys
from datetime import datetime
import psycopg2
import simplejson as json

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def main(conf_file, kpi_file):
	
	print('Connecting to Database...')

	conn = getDatabaseConnection(conf_file)
	cur = conn.cursor()
	kpi_scripts = open(kp_file).read().split('$$$')

	#print('Scripts: ' + kpi_scripts)
	print('Executing KPI scripts...')

	i = 1
	for kpi in kpi_scripts:
		print('Running block #' + str(i))
		cur.execute(kpi)
		conn.commit()
		i = i + 1

	print('Closing Database connection...')

	cur.close()
	conn.close()


# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	print('Being run as standalone!')
	conf_file = sys.argv[1] # Json file with Database connection details
	kpi_file = sys.argv[2]  # Text file with KPI calculation scripts to execute
	
	main(conf_file, kpi_file)