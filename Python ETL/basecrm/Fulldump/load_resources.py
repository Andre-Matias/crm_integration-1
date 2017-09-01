import psycopg2
import numpy as np
import time
import datetime
import simplejson as json

def getCopySql(schema, table, bucket, manifest, credentials):
    return "COPY %(schema)s.%(table)s\n" \
		"FROM '%(bucket)s'\n" \
		"JSON AS '%(manifest)s'\n" \
		"dateformat 'auto'\n" \
		"timeformat 'YYYY-MM-DDTHH:MI:SS'\n" \
		"gzip\n" \
		"CREDENTIALS '%(credentials)s';" \
		% {
		'schema': schema,
		'table': table,
		'bucket': bucket,
		'manifest': manifest,
		'credentials': credentials
	}

def getChandraConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])
	
def getS3Keys(conf_file):
	data = json.load(open(conf_file))
	return "aws_access_key_id=%(key)s;aws_secret_access_key=%(skey)s" \
	% {'key': data['s3_key'],'skey': data['s3_skey']}

def loadFromS3toRedshift(conf_file,schema,category,country,bucket,data_path,date,manifest_path,resources,prefix):
	conn = getChandraConnection(conf_file)
	credentials = getS3Keys(conf_file)

	cur = conn.cursor()

	if prefix == '':
		for resource in resources:
			print(resource)
			try:
				cur.execute(
					getCopySql(
						schema, \
						'%(prefix)sstg_d_base_%(resource)s' \
							% {
							'resource':resource,
							 'prefix': prefix},
						's3://%(bucket)s%(data_path)s%(resource)s/%(date)s/' \
							% {
							'resource':resource,
							'bucket':bucket,
							'date': date,
							'data_path':data_path},
						's3://%(bucket)s%(manifest_path)s%(prefix)s%(resource)s_jsonpath.json' \
							% {
							'prefix': prefix,
							'resource':resource,
							'bucket':bucket,
							'manifest_path':manifest_path
							}, 
						credentials
					)
				)
			except(psycopg2.InternalError):
				print("No file found")
				pass
	if prefix == 'sync_':
		for resource in resources:
			print(resource)
			try:
				cur.execute(
					getCopySql(
						schema, \
						'%(prefix)sstg_d_base_%(resource)s_%(category)s_%(country)s' \
							% {
							'resource':resource,
							'category':category,
							'country':country,
							 'prefix': prefix},
						's3://%(bucket)s%(data_path)s%(resource)s/%(date)s/' \
							% {
							'resource':resource,
							'bucket':bucket,
							'date': date,
							'data_path':data_path},
						's3://%(bucket)s%(manifest_path)s%(prefix)s%(resource)s_jsonpath.json' \
							% {
							'prefix': prefix,
							'resource':resource,
							'bucket':bucket,
							'manifest_path':manifest_path
							}, 
						credentials
					)
				)
			except(psycopg2.InternalError):
				print("No file found")
				pass
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def truncateResourceTables(conf_file,schema,resources,category,country,prefix):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	for resource in resources:
		cur.execute("TRUNCATE TABLE %(schema)s.%(prefix)sstg_d_base_%(resource)s_%(category)s_%(country)s" \
			% {
			'resource':resource,
			'category':category,
			'country':country,
			'prefix': prefix,
			'schema': schema
			}
		)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def deleteCategoryCountryDataFromTables(conf_file,schema,resources,category,country,prefix):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	for resource in resources:
		cur.execute("DELETE FROM %(schema)s.%(prefix)sstg_d_base_%(resource)s" \
			" WHERE base_account_country = '%(country)s'" \
			" AND base_account_category = '%(category)s'"  
			% {
			'resource':resource,
			'category':category,
			'country':country,
			'prefix': prefix,
			'schema': schema
			}
		)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()	

def syncDealsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute("CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.*  "\
			"FROM %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s AS sync_data  "\
			"INNER JOIN  "\
			"(SELECT  "\
			"id,  "\
			"max(meta_sequence) AS max_meta_sequence  "\
			"FROM %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_deals AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE (l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.meta_event_time as last_activity_at, "\
			"to_update_or_add.contact_id, "\
			"to_update_or_add.source_id, "\
			"to_update_or_add.estimated_close_date, "\
			"to_update_or_add.dropbox_email, "\
			"to_update_or_add.creator_id, "\
			"to_update_or_add.loss_reason_id, "\
			"to_update_or_add.currency, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"to_update_or_add.organization_id, "\
			"to_update_or_add.last_stage_change_at, "\
			"to_update_or_add.name, "\
			"to_update_or_add.owner_id, "\
			"to_update_or_add.value, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.hot, "\
			"to_update_or_add.last_stage_change_by_id, "\
			"to_update_or_add.stage_id, "\
			"to_update_or_add.custom_field_values, "\
			"to_update_or_add.tags "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_deals_history( "\
			"SELECT * FROM %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_deals WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN ( "\
			"SELECT id FROM %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_deals( "\
			"SELECT * FROM %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_deals_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncContactsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute("CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_contacts AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.creator_id, "\
			"to_update_or_add.contact_id, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"to_update_or_add.title, "\
			"to_update_or_add.name, "\
			"to_update_or_add.first_name, "\
			"to_update_or_add.last_name, "\
			"to_update_or_add.description, "\
			"to_update_or_add.industry, "\
			"to_update_or_add.website, "\
			"to_update_or_add.email, "\
			"to_update_or_add.phone, "\
			"to_update_or_add.mobile, "\
			"to_update_or_add.fax, "\
			"to_update_or_add.twitter, "\
			"to_update_or_add.facebook, "\
			"to_update_or_add.linkedin, "\
			"to_update_or_add.skype, "\
			"to_update_or_add.owner_id, "\
			"to_update_or_add.is_organization, "\
			"to_update_or_add.address, "\
			"to_update_or_add.custom_fields_values, "\
			"to_update_or_add.customer_status, "\
			"to_update_or_add.prospect_status, "\
			"to_update_or_add.tags "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_contacts_debug "\
			"( "\
			"SELECT * FROM %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_contacts WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN ( "\
			"SELECT id FROM %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_contacts "\
			"( "\
			"SELECT * FROM %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_contacts_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncLeadsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute("CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_leads AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.first_name, "\
			"to_update_or_add.last_name, "\
			"to_update_or_add.owner_id, "\
			"to_update_or_add.source_id, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"to_update_or_add.twitter, "\
			"to_update_or_add.phone, "\
			"to_update_or_add.mobile, "\
			"to_update_or_add.facebook, "\
			"to_update_or_add.email, "\
			"to_update_or_add.title, "\
			"to_update_or_add.skype, "\
			"to_update_or_add.linkedin, "\
			"to_update_or_add.description, "\
			"to_update_or_add.industry, "\
			"to_update_or_add.fax, "\
			"to_update_or_add.website, "\
			"to_update_or_add.address, "\
			"to_update_or_add.status, "\
			"to_update_or_add.creator_id, "\
			"to_update_or_add.organization_name, "\
			"to_update_or_add.custom_fields_values, "\
			"to_update_or_add.tags "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_leads_debug "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_leads WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_leads "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_leads_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncUsersTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute(
			"CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_users AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.name, "\
			"to_update_or_add.email, "\
			"to_update_or_add.role, "\
			"to_update_or_add.status, "\
			"to_update_or_add.confirmed, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"CASE to_update_or_add.status = 'inactive' "\
			"WHEN true THEN to_update_or_add.meta_event_time "\
			"ELSE NULL "\
			"END as deleted_at "\
			"from to_update_or_add "\
			"); "\
			"DELETE FROM %(schema)s.stg_d_base_users WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_users "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_users_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()
	
def syncCallsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute(
			"CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_calls AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.user_id, "\
			"to_update_or_add.phone_number, "\
			"to_update_or_add.missed, "\
			"to_update_or_add.associated_deals_ids, "\
			"to_update_or_add.resource_id, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"to_update_or_add.made_at, "\
			"to_update_or_add.summary, "\
			"to_update_or_add.outcome_id, "\
			"to_update_or_add.duration, "\
			"to_update_or_add.incoming, "\
			"to_update_or_add.recording_url, "\
			"to_update_or_add.resource_type "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_calls_debug "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_calls WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_calls "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_calls_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncTagsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute(
			"CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_tags AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.name, "\
			"to_update_or_add.creator_id, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at, "\
			"to_update_or_add.resource_type "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_tags_debug "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_tags WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_tags "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_tags_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncOrdersTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute(
			"CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_orders_%(category)s_in "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_orders AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.deal_id, "\
			"to_update_or_add.discount, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_orders "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_orders WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_orders "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_orders_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()

def syncLineItemsTable(conf_file,schema,category,country):
	conn = getChandraConnection(conf_file)
	cur = conn.cursor()

	### CREATE VIEW WITH NEW DATA
	cur.execute(
			"CREATE OR REPLACE VIEW %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s_view AS ( "\
			"WITH latest_changes AS ( "\
			"SELECT sync_data.* "\
			"FROM %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s AS sync_data "\
			"INNER JOIN "\
			"(SELECT "\
			"id, "\
			"max(meta_sequence) AS max_meta_sequence "\
			"FROM %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s "\
			"GROUP BY id) AS latest_data "\
			"ON (sync_data.id = latest_data.id AND sync_data.meta_sequence = latest_data.max_meta_sequence) "\
			"), "\
			"to_update_or_add AS ( "\
			"SELECT l_data.* "\
			"FROM latest_changes AS l_data "\
			"LEFT JOIN %(schema)s.stg_d_base_line_items AS p_data "\
			"ON (l_data.id = p_data.id) "\
			"WHERE ((l_data.meta_event_time > p_data.meta_event_time AND base_account_country = '%(country)s' AND base_account_category = '%(category)s') OR p_data.meta_event_time is null) "\
			") "\
			"select "\
			"'%(country)s' as base_account_country, "\
			"'%(category)s' as base_account_category, "\
			"to_update_or_add.meta_event_type, "\
			"to_update_or_add.meta_event_time, "\
			"to_update_or_add.id, "\
			"to_update_or_add.sku, "\
			"to_update_or_add.description, "\
			"to_update_or_add.order_id, "\
			"to_update_or_add.deal_id, "\
			"to_update_or_add.value, "\
			"to_update_or_add.price, "\
			"to_update_or_add.currency, "\
			"to_update_or_add.variation, "\
			"to_update_or_add.quantity, "\
			"to_update_or_add.name, "\
			"to_update_or_add.created_at, "\
			"to_update_or_add.meta_event_time as updated_at "\
			"from to_update_or_add "\
			"); "\
			"INSERT INTO %(schema)s.stg_d_base_line_items "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s_view); "\
			"DELETE FROM %(schema)s.stg_d_base_line_items WHERE base_account_country = '%(country)s' AND base_account_category = '%(category)s' "\
			"AND id IN (SELECT id FROM %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s_view); "\
			"INSERT INTO %(schema)s.stg_d_base_line_items "\
			"(SELECT * FROM %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s_view); "\
			"DROP VIEW %(schema)s.sync_stg_d_base_line_items_%(category)s_%(country)s_view;"
		% {
		'category':category,
		'country':country,
		'schema':schema
		}	
	)
	conn.commit()

	#Close connection
	cur.close()
	conn.close()