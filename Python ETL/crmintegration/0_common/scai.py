import sys
from datetime import datetime
import psycopg2
import simplejson as json


def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

	
def executeSQL(conf_file, sql_script, return_value=False):

	conn = getDatabaseConnection(conf_file)
	cur = conn.cursor()

	cur.execute(sql_script)
	conn.commit()

	if return_value:
		result = cur.fetchone()
		cur.close()
		conn.close()
		return result[0]
	else:	
		cur.close()
		conn.close()


# Step 1
def updateIntegrationStart(conf_file, cod_integration, cod_country):
	print('SCAI Step #1')
	sql_script = \
		"update crm_integration_anlt.t_rel_scai_country_integration "\
		"	set dat_processing = cast(to_char(trunc(sysdate),'yyyymmdd') as int), "\
		"	execution_nbr = case "\
		"						when trunc(sysdate) - to_date(dat_processing,'yyyymmdd') > 1 then 1 "\
		"	  					else execution_nbr + 1 "\
		"					  	end, "\
		"	cod_status = 2 "\
		"where "\
		"	cod_integration = %(cod_integration)d "\
		"	and cod_country = %(cod_country)d "\
		"	and ind_active = 1;" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country
	}
	#print(sql_script)
	executeSQL(conf_file, sql_script)

	
# Step 2
def insertIntegrationExecutionStart(conf_file, cod_integration, cod_country):
	print('SCAI Step #2')
	sql_script = \
		"insert into crm_integration_anlt.t_fac_scai_execution "\
		"select "\
		"	max_cod_exec + 1 cod_execution, "\
		"	cod_country, "\
		"	cod_integration, "\
		"	-1 cod_process, "\
		"	2 cod_status, "\
		"	1 cod_execution_type, "\
		"	dat_processing, "\
		"	execution_nbr, "\
		"	sysdate "\
		"from "\
		"	crm_integration_anlt.t_rel_scai_country_integration, "\
		"	(select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution) "\
		"where "\
		"	cod_integration = %(cod_integration)d "\
		"	and cod_country = %(cod_country)d "\
		"	and ind_active = 1;" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country			
	}
	#print(sql_script)	
	executeSQL(conf_file, sql_script)
	
		
# Step 3
def updateProcessStart(conf_file, dsc_process, cod_country):
	print('SCAI Step #3')
	sql_script = \
		"update crm_integration_anlt.t_rel_scai_integration_process "\
		"set dat_processing = source.dat_processing, execution_nbr = source.execution_nbr, cod_status = 2 "\
		"from "\
		" ( "\
		"	select proc.cod_process, rel_country_integr.dat_processing, rel_country_integr.cod_country, rel_country_integr.execution_nbr, rel_country_integr.cod_status, rel_country_integr.cod_integration "\
		"	from crm_integration_anlt.t_lkp_scai_process proc, crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, crm_integration_anlt.t_rel_scai_country_integration rel_country_integr "\
		"	where proc.dsc_process_short = '%(dsc_process)s' "\
		"	and proc.cod_process = rel_integr_proc.cod_process "\
		"	and rel_country_integr.cod_integration = rel_integr_proc.cod_integration "\
		"	and rel_country_integr.cod_country = rel_integr_proc.cod_country "\
		"	and rel_integr_proc.cod_country = %(cod_country)d "\
		"	and rel_integr_proc.ind_active = 1 "\
		" ) source "\
		"where crm_integration_anlt.t_rel_scai_integration_process.cod_process = source.cod_process "\
		"and crm_integration_anlt.t_rel_scai_integration_process.cod_country = source.cod_country "\
		"and crm_integration_anlt.t_rel_scai_integration_process.cod_integration = source.cod_integration;" \
	% {
		'dsc_process':dsc_process,
		'cod_country':cod_country
	}
	
	executeSQL(conf_file, sql_script)
	
	
# Step 4
def insertProcessExecutionStart(conf_file, dsc_process, cod_integration, cod_country):
	print('SCAI Step #4')
	sql_script = \
		"insert into crm_integration_anlt.t_fac_scai_execution "\
		"select "\
		"	max_cod_exec + 1 cod_execution, "\
		"	rel_integr_proc.cod_country, "\
		"	rel_integr_proc.cod_integration, "\
		"	rel_integr_proc.cod_process, "\
		"	rel_integr_proc.cod_status, "\
		"	1 cod_execution_type, "\
		"	rel_integr_proc.dat_processing, "\
		"	rel_integr_proc.execution_nbr, "\
		"	sysdate "\
		"from "\
		"	crm_integration_anlt.t_rel_scai_country_integration rel_country_integr, "\
		"	(select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution), "\
		"	crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, "\
		"	crm_integration_anlt.t_lkp_scai_process proc "\
		"where "\
		"	rel_country_integr.cod_integration = %(cod_integration)d "\
		"	and rel_country_integr.cod_country = %(cod_country)d "\
		"	and rel_country_integr.cod_integration = rel_integr_proc.cod_integration "\
		"	and rel_country_integr.cod_country = rel_integr_proc.cod_country "\
		"	and rel_integr_proc.cod_process = proc.cod_process "\
		"	and rel_integr_proc.cod_status = 2 "\
		"	and rel_country_integr.ind_active = 1 "\
		"	and rel_integr_proc.ind_active = 1 "\
		"	and proc.dsc_process_short = '%(dsc_process)s';" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country,
		'dsc_process':dsc_process
	}
	
	executeSQL(conf_file, sql_script)

		
# Step 5
def insertProcessExecutionEnd(conf_file, dsc_process, cod_integration, cod_country):
	print('SCAI Step #5')
	sql_script = \
		"insert into crm_integration_anlt.t_fac_scai_execution "\
		"select "\
		"	max_cod_exec + 1 cod_execution, "\
		"	rel_integr_proc.cod_country, "\
		"	rel_integr_proc.cod_integration, "\
		"	rel_integr_proc.cod_process, "\
		"	1 cod_status, "\
		"	2 cod_execution_type, "\
		"	rel_integr_proc.dat_processing, "\
		"	rel_integr_proc.execution_nbr, "\
		"	sysdate "\
		"from "\
		"	crm_integration_anlt.t_rel_scai_country_integration rel_country_integr, "\
		"	(select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution), "\
		"	crm_integration_anlt.t_rel_scai_integration_process rel_integr_proc, "\
		"	crm_integration_anlt.t_lkp_scai_process proc "\
		"where "\
		"	rel_country_integr.cod_integration = %(cod_integration)d "\
		"	and rel_country_integr.cod_country = %(cod_country)d "\
		"	and rel_country_integr.cod_integration = rel_integr_proc.cod_integration "\
		"	and rel_country_integr.cod_country = rel_integr_proc.cod_country "\
		"	and rel_integr_proc.cod_process = proc.cod_process "\
		"	and rel_integr_proc.cod_status = 2 "\
		"	and rel_country_integr.ind_active = 1 "\
		"	and rel_integr_proc.ind_active = 1 "\
		"	and proc.dsc_process_short = '%(dsc_process)s';" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country,
		'dsc_process':dsc_process
	}
	
	executeSQL(conf_file, sql_script)


# Step 6
def updateProcessEnd(conf_file, dsc_process, cod_country, table_name, date_column_name):
	print('SCAI Step #6')
	if date_column_name != '':
		sql_script = \
			"update crm_integration_anlt.t_rel_scai_integration_process "\
			"set cod_status = 1, "\
			"last_processing_datetime = isnull((select max(%(date_column_name)s) from crm_integration_stg.%(table_name)s),last_processing_datetime) "\
			"from crm_integration_anlt.t_lkp_scai_process proc "\
			"where t_rel_scai_integration_process.cod_process = proc.cod_process "\
			"and t_rel_scai_integration_process.cod_status = 2 "\
			"and t_rel_scai_integration_process.cod_country = %(cod_country)d "\
			"and proc.dsc_process_short = '%(dsc_process)s' "\
			"and t_rel_scai_integration_process.ind_active = 1;"\
		% {
			'cod_country':cod_country,
			'dsc_process':dsc_process,
			'table_name':table_name,
			'date_column_name':date_column_name
		}
	else:
		sql_script = \
			"update crm_integration_anlt.t_rel_scai_integration_process "\
			"set cod_status = 1, "\
			"last_processing_datetime = sysdate "\
			"from crm_integration_anlt.t_lkp_scai_process proc "\
			"where t_rel_scai_integration_process.cod_process = proc.cod_process "\
			"and t_rel_scai_integration_process.cod_status = 2 "\
			"and t_rel_scai_integration_process.cod_country = %(cod_country)d "\
			"and proc.dsc_process_short = '%(dsc_process)s' "\
			"and t_rel_scai_integration_process.ind_active = 1;"\
		% {
			'cod_country':cod_country,
			'dsc_process':dsc_process
		}
	executeSQL(conf_file, sql_script)
	
	
# Step 7
def insertIntegrationExecutionEnd(conf_file, cod_integration, cod_country):
	print('SCAI Step #7')
	sql_script = \
		"insert into crm_integration_anlt.t_fac_scai_execution "\
		"select "\
		"	max_cod_exec + 1 cod_execution, "\
		"	cod_country, "\
		"	cod_integration, "\
		"	-1 cod_process, "\
		"	1 cod_status, "\
		"	2 cod_execution_type, "\
		"	dat_processing, "\
		"	execution_nbr, "\
		"	sysdate "\
		"from "\
		"	crm_integration_anlt.t_rel_scai_country_integration, "\
		"	(select isnull(max(cod_execution),0) max_cod_exec from crm_integration_anlt.t_fac_scai_execution) "\
		"where "\
		"	cod_integration = %(cod_integration)d "\
		"	and cod_country = %(cod_country)d;" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country			
	}
	#print(sql_script)	
	executeSQL(conf_file, sql_script)


# Step 8
def updateIntegrationEnd(conf_file, cod_integration, cod_country):
	print('SCAI Step #8')
	sql_script = \
		"update crm_integration_anlt.t_rel_scai_country_integration "\
		"set "\
		"   cod_status = 1 "\
		"where "\
		"	cod_integration = %(cod_integration)d "\
		"	and cod_country = %(cod_country)d;" \
	% {
		'cod_integration':cod_integration,
		'cod_country':cod_country			
	}
	#print(sql_script)
	executeSQL(conf_file, sql_script)
	

# Steps 1 and 2, used before starting an integration
def integrationStart(conf_file, cod_integration, cod_country):
	updateIntegrationStart(conf_file, cod_integration, cod_country)
	insertIntegrationExecutionStart(conf_file, cod_integration, cod_country)
	
# Steps 7 and 8, used after ending an integration
def integrationEnd(conf_file, cod_integration, cod_country):
	insertIntegrationExecutionEnd(conf_file, cod_integration, cod_country)
	updateIntegrationEnd(conf_file, cod_integration, cod_country)

# Steps 3 and 4, used before starting a process
def processStart(conf_file, dsc_process, cod_integration, cod_country):
	updateProcessStart(conf_file, dsc_process, cod_country)
	insertProcessExecutionStart(conf_file, dsc_process, cod_integration, cod_country)

# Steps 5 and 6, used after ending a process
def processEnd(conf_file, dsc_process, cod_integration, cod_country, table_name='', date_column_name=''):
	insertProcessExecutionEnd(conf_file, dsc_process, cod_integration, cod_country)
	updateProcessEnd(conf_file, dsc_process, cod_country, table_name, date_column_name)
	
# Return the 'dsc_process_short' corresponding to the table name argument (TODO: Incorporate this 
def getProcessShortDescription(conf_file, table_name):
	sql_script = \
		"select "\
		"	dsc_process_short "\
		"from "\
		"	crm_integration_anlt.t_lkp_scai_process_tables a, "\
		"	crm_integration_anlt.t_lkp_scai_process b "\
		"where "\
		"	a.cod_process = b.cod_process "\
		"	and a.dsc_table = '%(table_name)s';" \
	% {
		'table_name':table_name		
	}

	process_name = executeSQL(conf_file, sql_script, return_value=True)
	print('SCAI Process: ' + process_name)
	
	return process_name
