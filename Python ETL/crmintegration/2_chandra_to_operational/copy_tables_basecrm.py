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


COD_INTEGRATION = 11000		# Chandra to Operational
COD_COUNTRY = -1			# Replaced by code in conf_file
COUNTRY = ''				# Replaced by name in conf_file
BASE_ACCOUNT_COUNTRY = -1	# Replaced by name in conf_file


def deletePreviousS3Files(conf_file, bucket_name, s3_path_prefix, scai_last_execution_status=1):

	if (scai_last_execution_status!=3):
		conf = json.load(open(conf_file))
		key = conf['s3_key']
		skey = conf['s3_skey']

		conn = S3Connection(key, skey)

		b = Bucket(conn, bucket_name)
		for x in b.list(prefix = s3_path_prefix):
			x.delete()

		
def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	print('Connecting to %(dbname)s at %(host)s' % { 'dbname':data['dbname'], 'host':data['host'] })
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

	
def getLastUpdateDates(db_conf_file, sc_schema, resources):
	print('Getting last update dates...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()
	
	last_updates_dict = dict()
	for resource in resources:
		target_table_name = 'stg_' + COUNTRY + '_' + resource[4:]	# Target table name has the country in the middle of the source table name (for example, stg_d_base_contacts -> stg_pt_d_base_contacts)
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
	
	
def copyFromDatabaseToS3(source_conf, target_conf, resources, schema, last_updates_dict, aux_path, scai_last_execution_status=1):
	print('Connecting to Chandra...')
	conn = getDatabaseConnection(source_conf)
	cur = conn.cursor()
	credentials = getS3Keys(source_conf)
	sc_conf = json.load(open(source_conf))
	
	#UNLOAD resources data	
	print('Unloading from Chandra...')
	for resource in resources:
		print('\t' + resource + ": " + last_updates_dict[resource])
		tg_table = 'stg_' + COUNTRY + '_' + resource[4:]	# Target table name has the country in the middle of the source table name (for example, stg_d_base_contacts -> stg_pt_d_base_contacts)
		scai_process_name = scai.getProcessShortDescription(target_conf, tg_table)				# SCAI
		
		if(scai_last_execution_status==3):
			scai_process_status = scai.processCheck(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
				
		# Is normal execution or re-execution starting from the step that was in error	
		if (scai_last_execution_status == 2 or (scai_last_execution_status == 3 and scai_process_status == 3)):
			scai.processStart(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY)			# SCAI
			try:
				cur.execute(
					"UNLOAD ('SELECT * from %(schema)s.%(resource)s "\
					"		WHERE meta_event_time >= \\\'%(last_update_date)s\\\' "\
					"		AND base_account_country = \\\'%(BASE_ACCOUNT_COUNTRY)s\\\'') "\
					"TO 's3://%(aux_path)s/%(schema)s_%(resource)s/data_' "\
					"CREDENTIALS '%(credentials)s' "\
					"ESCAPE "\
					"manifest;"
				% {
				'schema':schema,
				'resource':resource,
				'last_update_date':last_updates_dict[resource],
				'credentials':credentials,
				'aux_path':aux_path,
				'BASE_ACCOUNT_COUNTRY':BASE_ACCOUNT_COUNTRY
				}		
				)
			except Exception as e:
				conn.rollback()
				scai.processEnd(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'meta_event_time',3)	# SCAI
				scai.integrationEnd(target_conf, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
				print (e)
				print (e.pgerror)
				sys.exit("The process aborted with error.")
			else:
				conn.commit()
				scai.processEnd(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'meta_event_time',1)	# SCAI


	#Close connection
	cur.close()
	conn.close()

	
def copyFromS3ToDatabase(target_conf, resources, sc_schema, tg_schema, aux_path, scai_last_execution_status=1):		
	#LOAD to target redshift
	print('Connecting to Yamato...')
	conn_target = getDatabaseConnection(target_conf)
	cur_target = conn_target.cursor()
	credentials = getS3Keys(target_conf)

	print('Loading to Yamato...')
	for resource in resources:
		tg_table = 'stg_' + COUNTRY + '_' + resource[4:]	# Target table name has the country in the middle of the source table name (for example, stg_d_base_contacts -> stg_pt_d_base_contacts)
		print('Loading %(tg_schema)s.%(tg_table)s...' % {'tg_schema':tg_schema, 'tg_table':tg_table })
		scai_process_name = scai.getProcessShortDescription(target_conf, tg_table)				# SCAI
		
		if(scai_last_execution_status==3):
			scai_process_status = scai.processCheck(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
				
		# Is normal execution or re-execution starting from the step that was in error	
		if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):
			scai.processStart(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY)			# SCAI
			try:
				cur_target.execute(
					"TRUNCATE TABLE %(tg_schema)s.%(tg_table)s; "\
					"COPY %(tg_schema)s.%(tg_table)s "\
					"FROM 's3://%(aux_path)s/%(sc_schema)s_%(resource)s/data_manifest' "\
					"CREDENTIALS '%(credentials)s' "\
					"REGION 'us-west-2' "\
					"ESCAPE "\
					"manifest; "\
					"ANALYZE %(tg_schema)s.%(tg_table)s;"
				% {
				'tg_schema':tg_schema,
				'tg_table':tg_table,
				'resource':resource,
				'credentials':credentials,
				'aux_path':aux_path,
				'sc_schema':sc_schema
				}	
				)
			except Exception as e:
				conn_target.rollback()
				scai.processEnd(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'meta_event_time',3)	# SCAI
				scai.integrationEnd(target_conf, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
				print (e)
				print (e.pgerror)
				sys.exit("The process aborted with error.")
			else:
				conn_target.commit()
				scai.processEnd(target_conf, scai_process_name, COD_INTEGRATION, COD_COUNTRY, tg_table, 'meta_event_time',1)	# SCAI
				
				#Enable execution of following processes
				scai_last_execution_status = 1

	cur_target.close()
	conn_target.close()
	
	
def main(conf_file, source_conf_file, target_conf_file, scai_last_execution_status):
	print(datetime.now().time())
	
	data = json.load(open(conf_file))
	sc_schema = data['source_schema']
	tg_schema = data['target_schema']
	resources = data['resources'].split(',')
	
	global COD_COUNTRY; COD_COUNTRY = int(data['cod_country'])							# Global variable
	global COUNTRY; COUNTRY = data['country']											# Global variable
	global BASE_ACCOUNT_COUNTRY; BASE_ACCOUNT_COUNTRY = data['base_account_country']	# Global variable
	
	# Delete old S3 files
	print('Deleting old S3 files from bucket ' + data['bucket_name'] + ' and path ' + data['aux_s3_path_prefix'] + '...')
	deletePreviousS3Files(source_conf_file, data['bucket_name'], data['aux_s3_path_prefix'], scai_last_execution_status)

	print(datetime.now().time())

	# Copy from source database to S3
	print('\nResources to unload:\n' + str(resources) + '\n')
	last_updates_dict = getLastUpdateDates(target_conf_file, sc_schema, resources)	# Get the date of last update for each of this schema's resources
	copyFromDatabaseToS3(source_conf_file, target_conf_file, resources, sc_schema, last_updates_dict, data['aux_s3_path'], scai_last_execution_status)

	print(datetime.now().time())
	
	# Copy from S3 to target database
	print('\nResources to load:\n' + str(resources) + '\n')
	copyFromS3ToDatabase(target_conf_file, resources, sc_schema, tg_schema, data['aux_s3_path'], scai_last_execution_status)

	print(datetime.now().time())

	
# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	# Get information from configuration files
	conf_file = sys.argv[1] 		# File with names for the tables to copy and S3 path
	source_conf_file = sys.argv[2] 	# File with source database
	target_conf_file = sys.argv[3] 	# File with target database

	main(conf_file, source_conf_file, target_conf_file)