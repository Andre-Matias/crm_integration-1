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

	
def copyAtlasTables(db_conf_file, sc_schema, tg_schema, resources, last_updates_dict, verticals_names='', scai_last_execution_status=1):
	print('Connecting to Yamato...')
	conn_target = getDatabaseConnection(db_conf_file)
	cur_target = conn_target.cursor()
	
	if(sc_schema == 'db_atlas'):	
		for resource in resources:	
			tg_table = 'stg_%(COUNTRY)s_%(sc_schema)s_%(resource)s' % {'resource':resource, 'sc_schema':sc_schema, 'COUNTRY':COUNTRY}
			scai_process_name = scai.getProcessShortDescription(db_conf_file, tg_table)			# SCAI
			if(scai_last_execution_status==3):
				scai_process_status = scai.processCheck(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI

			# Is normal execution or re-execution starting from the step that was in error	
			if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):
				scai.processStart(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI
				print('Loading %(tg_schema)s.%(tg_table)s from %(last_update)s...' % {'tg_schema':tg_schema, 'tg_table':tg_table, 'last_update':last_updates_dict[resource]})
				try:
					cur_target.execute(
						"TRUNCATE TABLE %(tg_schema)s.%(tg_table)s; "\
						"INSERT INTO %(tg_schema)s.%(tg_table)s "\
						"SELECT * FROM %(sc_schema)s.%(resource)s "\
						"WHERE operation_timestamp >= '%(last_update_date)s'; "\
						"ANALYZE %(tg_schema)s.%(tg_table)s;"
					% {
					'tg_table':tg_table,
					'tg_schema':tg_schema,
					'sc_schema':sc_schema,
					'resource':resource,
					'last_update_date':last_updates_dict[resource]
					}	
					) 
				except Exception as e:
					conn_target.rollback()
					scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'operation_timestamp',3)	# SCAI
					scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
					print (e)
					print (e.pgerror)
					sys.exit("The process aborted with error.")
				else:
					conn_target.commit()
					scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'operation_timestamp',1)	# SCAI
					
					#Enable execution of following processes
					scai_last_execution_status = 1
			
	elif(sc_schema == 'db_atlas_verticals'):
		for resource in resources:
			tg_table = 'stg_%(COUNTRY)s_%(sc_schema)s_%(resource)s' % {'resource':resource, 'sc_schema':sc_schema, 'COUNTRY':COUNTRY}
			scai_process_name = scai.getProcessShortDescription(db_conf_file, tg_table)			# SCAI
			if(scai_last_execution_status==3):
				scai_process_status = scai.processCheck(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
				
			# Is normal execution or re-execution starting from the step that was in error	
			if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):
				scai.processStart(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI
				 	
				print('Loading %(tg_schema)s.%(tg_table)s from %(last_update)s...' % {'tg_schema':tg_schema, 'tg_table':tg_table, 'last_update':last_updates_dict[resource]})
				try:
					cur_target.execute(
						"TRUNCATE TABLE %(tg_schema)s.%(tg_table)s; "\
						"INSERT INTO %(tg_schema)s.%(tg_table)s "\
						"SELECT * FROM %(sc_schema)s.%(resource)s "\
						"WHERE operation_timestamp >= '%(last_update_date)s' "\
						"AND livesync_dbname in (%(verticals_names)s); "\
						"ANALYZE %(tg_schema)s.%(tg_table)s;"
					% {
					'tg_table':tg_table,
					'tg_schema':tg_schema,
					'sc_schema':sc_schema,
					'resource':resource,
					'verticals_names':verticals_names,
					'last_update_date':last_updates_dict[resource]
					}	
					)
				except Exception as e:
					conn_target.rollback()
					scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'operation_timestamp',3)	# SCAI
					scai.integrationEnd(db_conf_file, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
					print (e)
					print (e.pgerror)
					sys.exit("The process aborted with error.")
				else:
					conn_target.commit()
					scai.processEnd(db_conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'operation_timestamp',1)	# SCAI
					
					#Enable execution of following processes
					scai_last_execution_status = 1
	
	cur_target.close()
	conn_target.close()


def main(conf_file, db_conf_file, scai_last_execution_status):
	print(datetime.now().time())
	
	data = json.load(open(conf_file))
	sc_schemata = data['source_schemata_atlas'].split(',')
	tg_schema = data['target_schema_operational']
	verticals_names = data['verticals_names']
	
	global COD_COUNTRY; COD_COUNTRY = int(data['cod_country'])												# Global variable
	global COUNTRY; COUNTRY = data['country']																# Global variable
	print('First Last execution status= %(scai_last_execution_status)s;  ' % {'scai_last_execution_status':scai_last_execution_status})	
		
	# Copy tables from several source schemata to single target schema
	for sc_schema in sc_schemata:
		resources_identifier = 'resources_' + sc_schema
		resources = data[resources_identifier].split(',')
		print('\n' + resources_identifier)
		last_updates_dict = getLastUpdateDates(db_conf_file, sc_schema, resources)							# Get the date of last update for each of this schema's resources
		copyAtlasTables(db_conf_file, sc_schema, tg_schema, resources, last_updates_dict, verticals_names, scai_last_execution_status)	# Copy Yamato tables to Operational Model, from dates of last update

	print('Done copying all Atlas tables!')
	print(datetime.now().time())

	
# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	# Get information from configuration files
	conf_file = sys.argv[1] 	# File with names for the tables to copy
	db_conf_file = sys.argv[2] 	# File with database connection

	main(conf_file, db_conf_file)