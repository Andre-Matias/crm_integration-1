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

def main(db_conf_file, kpi_file, country):
	
	print('Connecting to Database...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()
	
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
	

	kpi_scripts = open(kpi_file).read().split('$$$')

	#print('Scripts: ' + kpi_scripts)
	print('Executing KPI scripts...')

	i = 1
	for kpi in kpi_scripts:
		if i < block_nbr:  # Make this run starting from a certain block
			i = i + 1
			continue
		print('Running block #' + str(i)) 
		try:
			cur.execute(kpi)
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
	

# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	db_conf_file = sys.argv[1] 	# Database configuration file
	kpi_file = sys.argv[2]  	# File with KPI calculation scripts to execute
	country = int(sys.argv[3])  # Country code
	
	main(db_conf_file, kpi_file, country)