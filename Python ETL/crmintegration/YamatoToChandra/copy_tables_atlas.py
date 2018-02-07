import sys
import simplejson as json
from datetime import date, datetime
import psycopg2
import numpy as np
import time
from boto.s3.connection import S3Connection, Bucket, Key
import scai

COD_INTEGRATION = 10000		# Yamato to Chandra
COD_COUNTRY = 1				# Portugal

def deletePreviousS3Files(conf_file):
	conf = json.load(open(conf_file))
	key = conf['s3_key']
	skey = conf['s3_skey']

	conn = S3Connection(key, skey)
	"""b = Bucket(conn, 'verticals-raw-data')
	for x in b.list(prefix = 'BaseCRM_v3/Aux/'):
		x.delete()
	"""
	b = Bucket(conn, 'pyrates-data-ocean')
	for x in b.list(prefix = 'andre-matias/Aux/'):
		x.delete()

		
def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	#print('Connecting to %(dbname)s at %(host)s' % { 'dbname':data['dbname'], 'host':data['host'])
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

	
def getLastUpdateDates(database_conf, sc_schema, resources):
	print('Getting last update dates...')
	conn = getDatabaseConnection(database_conf)
	cur = conn.cursor()
	
	last_updates_dict = dict()
	for resource in resources:
		target_table_name = 'stg_' + sc_schema + '_' + resource
		cur.execute(
			"select isnull(rel_integr_proc.last_processing_datetime,'1900-01-01 00:00:00.000000') "\
			"from sandbox_andre_matias.t_lkp_scai_process proc, sandbox_andre_matias.t_rel_scai_integration_process rel_integr_proc "\
			"where rel_integr_proc.cod_process = proc.cod_process "\
			"and rel_integr_proc.cod_country = %(COD_COUNTRY)d "\
			"and rel_integr_proc.cod_integration = %(COD_INTEGRATION)d "\
			"and rel_integr_proc.ind_active = 1 "\
			"and proc.dsc_process_short = '%(target_table_name)s' "\
			% {
				'COD_COUNTRY':COD_COUNTRY,
				'COD_INTEGRATION':COD_INTEGRATION,
				'target_table_name':target_table_name
			}
		)
		last_updates_dict[resource] = cur.fetchone()[0].isoformat()
		print('\t' + target_table_name + ': ' + last_updates_dict[resource])

	cur.close()
	conn.close()

	return last_updates_dict
	
	
def copyFromDatabaseToS3(source_conf, target_conf, resources, schema, last_updates_dict):
	print('Connecting to Yamato...')
	conn = getDatabaseConnection(source_conf)
	cur = conn.cursor()
	credentials = getS3Keys(source_conf)
	sc_conf = json.load(open(source_conf))

	aux_path = sc_conf['aux_s3_path']
	
	#UNLOAD resources data	
	if(schema == 'db_atlas'):
		print('Unloading from Yamato (atlas)...')
		for resource in resources:
			print('\t' + resource + ": " + last_updates_dict[resource])
			scai.processStart(target_conf, 'stg_%(sc_schema)s_%(resource)s' % {'resource':resource,'sc_schema':schema}, COD_INTEGRATION, COD_COUNTRY) # SCAI
			cur.execute(
				"UNLOAD ('select * from %(schema)s.%(resource)s "\
				"		where operation_timestamp >= \\\'%(last_update_date)s\\\'') "\
				"to 's3://%(aux_path)s/%(schema)s_%(resource)s/data_' "\
				"CREDENTIALS '%(credentials)s' "\
				"ESCAPE "\
				"manifest;"
			% {
			'schema':schema,
			'resource':resource,
			'last_update_date':last_updates_dict[resource],
			'credentials':credentials,
			'aux_path':aux_path
			}		
			)
			conn.commit()
	
	elif(schema == 'db_atlas_verticals'):
		print('Unloading from Yamato (atlas_verticals)...')
		for resource in resources:
			print('\t' + resource + ": " + last_updates_dict[resource])
			scai.processStart(target_conf, 'stg_%(sc_schema)s_%(resource)s' % {'resource':resource, 'sc_schema':schema}, COD_INTEGRATION, COD_COUNTRY) # SCAI
			cur.execute(
				"UNLOAD ('select * from %(schema)s.%(resource)s "\
				"		where operation_timestamp >= \\\'%(last_update_date)s\\\' "\
				"		and livesync_dbname in (\\\'imovirtualpt\\\', \\\'carspt\\\')') "\
				"to 's3://%(aux_path)s/%(schema)s_%(resource)s/data_' "\
				"CREDENTIALS '%(credentials)s' "\
				"ESCAPE "\
				"manifest;"
			% {
			'schema':schema,
			'resource':resource,
			'last_update_date':last_updates_dict[resource],
			'credentials':credentials,
			'aux_path':aux_path
			}		
			)
			conn.commit()
	
	#Close connection
	cur.close()
	conn.close()

	
def copyFromS3ToDatabase(target_conf, resources, sc_schema):		
	#LOAD to target redshift
	print('Connecting to Chandra...')
	conn_target = getDatabaseConnection(target_conf)
	cur_target = conn_target.cursor()
	credentials = getS3Keys(target_conf)

	tg_conf = json.load(open(target_conf))
	tg_schema = tg_conf['redshift_schema']
	aux_path = tg_conf['aux_s3_path']

	print('Loading to Chandra...')
	for resource in resources:
		print('Loading %(tg_schema)s.stg_%(sc_schema)s_%(resource)s' % {'tg_schema':tg_schema, 'resource':resource, 'sc_schema':sc_schema })
		cur_target.execute(
			"TRUNCATE TABLE %(tg_schema)s.stg_%(sc_schema)s_%(resource)s; "\
			"COPY %(tg_schema)s.stg_%(sc_schema)s_%(resource)s "\
			"from 's3://%(aux_path)s/%(sc_schema)s_%(resource)s/data_manifest' "\
			"CREDENTIALS '%(credentials)s' "\
			"ESCAPE "\
			"manifest;"
		% {
		'tg_schema':tg_schema,
		'resource':resource,
		'credentials':credentials,
		'aux_path':aux_path,
		'sc_schema':sc_schema
		}	
		)
		conn_target.commit()
		scai.processEnd(target_conf, 'stg_%(sc_schema)s_%(resource)s' % {'resource':resource,'sc_schema':sc_schema}, COD_INTEGRATION, COD_COUNTRY, 'operation_timestamp') # SCAI

	cur_target.close()
	conn_target.close()
	
	
def main(conf_file, source_conf_file, target_conf_file):
	print(datetime.now().time())
	
	data = json.load(open(conf_file))
	sc_schemata = json.load(open(source_conf_file))['redshift_schemas'].split(',')

	# Delete old S3 files
	print('Deleting old S3 files...')
	deletePreviousS3Files(source_conf_file)

	print(datetime.now().time())

	# Copy from source database to S3 (several source schemata)
	for sc_schema in sc_schemata:
		resources_identifier = 'resources_' + sc_schema
		resources = data[resources_identifier].split(',')
		print('\n' + resources_identifier)
		last_updates_dict = getLastUpdateDates(target_conf_file, sc_schema, resources)	# Get the date of last update for each of this schema's resources
		copyFromDatabaseToS3(source_conf_file, target_conf_file, resources, sc_schema, last_updates_dict)

	#print(datetime.now().time())
	#input("Everything copied from Yamato to S3. Proceed copying to Chandra?\n")
	print(datetime.now().time())

	# Copy from S3 to target database (single target schema)
	for sc_schema in sc_schemata:
		resources_identifier = 'resources_' + sc_schema
		resources = data[resources_identifier].split(',')
		print('\n' + resources_identifier)
		copyFromS3ToDatabase(target_conf_file, resources, sc_schema)

	print(datetime.now().time())

	
# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	print('Being run as standalone!')
	# Get information from configuration files
	conf_file = sys.argv[1] #File with names for the tables to copy
	source_conf_file = sys.argv[2] #File with S3 path and source database
	target_conf_file = sys.argv[3] #File with target database

	main(conf_file, source_conf_file, target_conf_file)
