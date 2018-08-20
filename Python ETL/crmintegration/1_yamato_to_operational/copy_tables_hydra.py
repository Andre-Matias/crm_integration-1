import sys
import simplejson as json
from datetime import date, datetime
import psycopg2
import numpy as np
import time
from boto.s3.connection import S3Connection, Bucket, Key
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai


COD_INTEGRATION = 10000		# Yamato to Operational
COD_COUNTRY = -1			# Replaced by code in conf_file
COUNTRY = ''				# Replaced by name in conf_file
HYDRA_COUNTRY_CODE = ''		# Replaced by name in conf_file

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	print('Connecting to %(dbname)s at %(host)s' % { 'dbname':data['dbname'], 'host':data['host'] })
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])


def getLastUpdateDates(db_conf_file, sc_schema, resources):
	print('Getting last update dates...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()
	
	last_updates_dict = dict()
	for resource in resources:
		target_table_name = 'stg_' + COUNTRY + '_' + sc_schema + '_' + resource
		scai_process_name = scai.getProcessShortDescription(db_conf_file, target_table_name)
		cur.execute(
			"SELECT isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') "\
			"FROM crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc "\
			"WHERE rel_integr_proc.cod_process = proc.cod_process "\
			"AND rel_integr_proc.cod_country = %(COD_COUNTRY)d "\
			"AND rel_integr_proc.cod_integration = %(COD_INTEGRATION)d "\
			"AND rel_integr_proc.ind_active = 1 "\
			"AND proc.dsc_process_short = '%(scai_process_name)s' "\
			% {
				'COD_COUNTRY':COD_COUNTRY,
				'COD_INTEGRATION':COD_INTEGRATION,
				'scai_process_name':scai_process_name
			}
		)
		last_updates_dict[resource] = cur.fetchone()[0].isoformat()
		print('\t' + target_table_name + ': ' + last_updates_dict[resource])

	cur.close()
	conn.close()

	return last_updates_dict


def copyHydraTable(db_conf_file, sc_schema, tg_schema, resource, last_update_date, horizontal_name, scai_last_execution_status=1):	
	print('Connecting to Yamato...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()

	tg_table = 'stg_%(COUNTRY)s_%(sc_schema)s_%(resource)s' % {'resource':resource, 'sc_schema':sc_schema, 'COUNTRY':COUNTRY}
	scai_process_name = scai.getProcessShortDescription(db_conf_file, tg_table)			# SCAI
	if(scai_last_execution_status==3):
		scai_process_status = scai.processCheck(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
		
	# Is normal execution or re-execution starting from the step that was in error	
	if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):	
		scai.processStart(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI
		print('Loading %(tg_schema)s.%(tg_table)s from %(last_update)s...' % {'tg_schema':tg_schema, 'tg_table':tg_table, 'last_update':last_update_date})
		try:
			cur.execute(
				"TRUNCATE TABLE %(tg_schema)s.%(tg_table)s; "\
				"INSERT INTO %(tg_schema)s.%(tg_table)s "\
				"SELECT "\
				"	server_date_day, "\
				"	ad_id, "\
				"	action_type, "\
				"	%(horizontal_name)s source, "\
				"	count(*) occurrences, "\
				"	count(distinct session_long) distinct_occurrences "\
				"FROM hydra.web "\
				"WHERE upper(country_code) = '%(HYDRA_COUNTRY_CODE)s' "\
				"AND ad_id is not null "\
				"AND server_date_day >= '%(last_update_date)s' "\
				"GROUP BY server_date_day, ad_id, action_type; "\
				"ANALYZE %(tg_schema)s.%(tg_table)s;"
			% {
			'tg_table':tg_table,
			'tg_schema':tg_schema,
			'horizontal_name':horizontal_name,
			'HYDRA_COUNTRY_CODE':HYDRA_COUNTRY_CODE,
			'last_update_date':last_update_date
			}		
			)
		except Exception as e:
			conn.rollback()
			scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'server_date_day',3)	# SCAI
			scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
			print (e)
			print (e.pgerror)
			sys.exit("The process aborted with error.")
		else:
			conn.commit()
			scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'server_date_day',1)

			#Enable execution of following processes
			scai_last_execution_status = 1			# SCAI

	cur.close()
	cur.close()
	
	# If error was solved here, return new status to use in subsequent processes
	return scai_last_execution_status


def copyHydraVerticalsTable(db_conf_file, sc_schema, tg_schema, resource, last_update_date, hydra_verticals_names, anlt_verticals_names, scai_last_execution_status=1):		
	print('Connecting to Yamato...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()
	
	tg_table = 'stg_%(COUNTRY)s_%(sc_schema)s_%(resource)s' % {'resource':resource, 'sc_schema':sc_schema, 'COUNTRY':COUNTRY}
	scai_process_name = scai.getProcessShortDescription(db_conf_file, tg_table)			# SCAI
	
	if(scai_last_execution_status==3):
		scai_process_status = scai.processCheck(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
		
	# Is normal execution or re-execution starting from the step that was in error	
	if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):	
		scai.processStart(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI
		print('Loading %(tg_schema)s.%(tg_table)s from %(last_update)s...' % {'tg_schema':tg_schema, 'tg_table':tg_table, 'last_update':last_update_date})
		
		# Dynamically build CASE statement according to number of verticals
		case_statement = "CASE"
		for i in range(len(anlt_verticals_names)):
			case_statement += " WHEN lower(host) LIKE '%%" + hydra_verticals_names[i] + "%%' THEN " + anlt_verticals_names[i]
		case_statement += " ELSE 'other' END"
		
		try:
			cur.execute(
				"TRUNCATE TABLE %(tg_schema)s.%(tg_table)s; "\
				"INSERT INTO %(tg_schema)s.%(tg_table)s "\
				"SELECT "\
				"	server_date_day, "\
				"	ad_id, "\
				"	trackname, "\
				"	%(case_statement)s source, "\
				"	count(*) occurrences, "\
				"	count(distinct session_long) distinct_occurrences "\
				"FROM hydra_verticals.web "\
				"WHERE upper(country_code) = '%(HYDRA_COUNTRY_CODE)s' "\
				"AND ad_id is not null "\
				"AND server_date_day >= '%(last_update_date)s' "\
				"GROUP BY server_date_day, ad_id, trackname, "\
				"	%(case_statement)s; "\
				"ANALYZE %(tg_schema)s.%(tg_table)s;"
			% {
			'tg_table':tg_table,
			'tg_schema':tg_schema,
			'HYDRA_COUNTRY_CODE':HYDRA_COUNTRY_CODE,
			'last_update_date':last_update_date,
			'case_statement':case_statement
			}
			)
		except Exception as e:
			conn.rollback()
			scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'server_date_day',3)	# SCAI
			scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
			print (e)
			print (e.pgerror)
			sys.exit("The process aborted with error.")
		else:
			conn.commit()
			scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'server_date_day',1)	# SCAI
			
			#Enable execution of following processes
			scai_last_execution_status = 1

	cur.close()
	cur.close()
	
	# If error was solved here, return new status to use in subsequent processes
	return scai_last_execution_status
	
		
def main(conf_file, db_conf_file, scai_last_execution_status):
	print(datetime.now().time())
	
	data = json.load(open(conf_file))
	resource = 'web'																								# There is only 1 resource for hydra and hydra_verticals
	sc_schema_hydra = 'hydra'
	sc_schema_hydra_verticals = 'hydra_verticals'
	tg_schema = 'crm_integration_stg'
	anlt_verticals_names = data['verticals_names'].split(',')														# List with names of verticals platforms according to our Analytical Model (with apostrophes)
	hydra_verticals_names = data['hydra_verticals_names'].split(',')												# List with names of verticals platforms according to Hydra tables data (should be in same order as the ones above and map 1 for 1)
	horizontal_name = data['horizontal_name']																		# Name of horizontal platform according to our Analytical Model (with apostrophes)
	
	# Check if the mapping is correct
	if len(anlt_verticals_names) != len(hydra_verticals_names):
		raise NameError('Number of verticals names for analytical model and hydra tables do not match!')
	
	global COD_COUNTRY; COD_COUNTRY = int(data['cod_country'])														# Global variable
	global COUNTRY; COUNTRY = data['country']																		# Global variable
	global HYDRA_COUNTRY_CODE; HYDRA_COUNTRY_CODE = data['hydra_country_code']										# Global variable
	
 
	# Copy tables 'web' from schema 'hydra' to Operational Model
	last_update_date = getLastUpdateDates(db_conf_file, sc_schema_hydra, [resource])[resource]						# Function returns as dictionary, so we need to index by the key 'web' (in 'resource' variable)
	scai_last_execution_status = copyHydraTable(db_conf_file, sc_schema_hydra, tg_schema, resource, last_update_date, horizontal_name,scai_last_execution_status)			# Function that effectively copies 'hydra.web' table
	print(datetime.now().time())
	if(len(hydra_verticals_names) > 0):
		# Copy tables 'web' from schema 'hydra_verticals' to Operational Model
		last_update_date = getLastUpdateDates(db_conf_file, sc_schema_hydra_verticals, [resource])[resource]			# Function returns as dictionary, so we need to index by the key 'web' (in 'resource' variable)
		scai_last_execution_status = copyHydraVerticalsTable(db_conf_file, sc_schema_hydra_verticals, tg_schema, resource, last_update_date, hydra_verticals_names, anlt_verticals_names,scai_last_execution_status)	# Function that effectively copies 'hydra_verticals.web' table


	print('Done copying all Hydra tables!')
	print(datetime.now().time())
	
	# If error was solved here, return new status to use in subsequent processes
	return scai_last_execution_status
	

# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	# Get information from configuration files
	conf_file = sys.argv[1] 	# File with names for the tables to copy
	db_conf_file = sys.argv[2] 	# File with database connection
	
	main(conf_file, db_conf_file)