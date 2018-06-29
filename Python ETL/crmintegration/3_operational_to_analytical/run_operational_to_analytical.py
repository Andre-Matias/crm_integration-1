import sys
from datetime import datetime
import psycopg2
import simplejson as json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai

COD_INTEGRATION = 30000		# Operational to Analytical

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def main(conf_file, dml_file, country):
	print(datetime.now().time())
	print('Connecting to Database...')
	
	conn = getDatabaseConnection(conf_file)
	cur = conn.cursor()
	
	scai_last_execution_status = scai.getLastExecutionStatus(conf_file, COD_INTEGRATION, country)	# SCAI

	if (scai_last_execution_status == 2):
		sys.exit("The integration is already running...")

    #If last execution ended in error, then check in which block it ended
	block_nbr = cur.execute("select "\
				" nvl(block_nbr,1) as block_nbr "\
				" from crm_integration_anlt.t_rel_scai_country_integration country_integration"\
				"where "\
				"	country_integration.cod_integration = %(COD_INTEGRATION)d "\
				"	and country_integration.cod_country = %(cod_country)d "\
				"   and country_integration.cod_status = 3 "\
				"	and ind_active = 1 "\
				% {
					'cod_country':country ,
					'COD_INTEGRATION':COD_INTEGRATION
				}
			)
	conn.commit()
	
	#If above query does not return a value (For example on a normal execution, without previous errors)
	if (not block_nbr):
		block_nbr = 1
	
	scai.integrationStart(conf_file, COD_INTEGRATION, country) 	# SCAI
	

	dml_scripts = open(dml_file).read().split('$$$')
	
	#print('Scripts: ' + dml_scripts)
	print('Executing DML scripts...')
	i = 1
	for dml in dml_scripts:
		if i < block_nbr:  # Make this run starting from a certain block
			i = i + 1
			continue
		print('Running block #' + str(i))
		try:
			cur.execute(dml)
		except Exception as e:
			conn.rollback() 
			scai.integrationEnd(db_conf_file, COD_INTEGRATION, country, 3, i)		# SCAI
			print (e)
			print (e.pgerror)
			sys.exit("The process aborted with error.")
		else:
			conn.commit() 

		i = i + 1

		
	print('Closing Database connection...')
	cur.close()
	conn.close()
	scai.integrationEnd(db_conf_file, COD_INTEGRATION, country, 1)		# SCAI
	print(datetime.now().time())
	print('All done!')

# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	conf_file = sys.argv[1] # Json file with Database connection details
	dml_file = sys.argv[2]  # Text file with DML scripts to execute
	country = sys.argv[3]  # Text file with DML scripts to execute
	
	main(conf_file, dml_file, country)