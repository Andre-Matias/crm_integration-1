import sys
from datetime import datetime
import psycopg2
import simplejson as json

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def main(conf_file, dml_file):
	print(datetime.now().time())
	print('Connecting to Database...')

	conn = getDatabaseConnection(conf_file)
	cur = conn.cursor()
	dml_scripts = open(dml_file).read().split('$$$')

	#print('Scripts: ' + dml_scripts)
	print('Executing DML scripts...')

	i = 1
	for dml in dml_scripts:
		#if i < 158:  # Remove comments to make this run starting from a certain block
		#	i = i + 1
		#	continue
		print('Running block #' + str(i))
		cur.execute(dml)
		conn.commit()
		i = i + 1

	print('Closing Database connection...')
	cur.close()
	conn.close()
	print(datetime.now().time())
	print('All done!')

# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	conf_file = sys.argv[1] # Json file with Database connection details
	dml_file = sys.argv[2]  # Text file with DML scripts to execute
	
	main(conf_file, dml_file)