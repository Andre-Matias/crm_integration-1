from __future__ import unicode_literals
import basecrm
import boto
from boto.s3.key import Key
from munch import *
import simplejson as json
from decimal import *
import os
import sys
import gzip
from datetime import datetime, timedelta
import requests
import time
import psycopg2
import numpy as np
from boto.s3.connection import S3Connection, Bucket, Key
from retry import retry
import logging 
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))   
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'crmintegration', '0_common'))  
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..','crmintegration', '0_common'))  
import scai
import base_auto_task

 
def getDatabaseConnection(DB_CONF_FILE):
	data = json.load(open(DB_CONF_FILE))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def executeSQL(DB_CONF_FILE, sql_script, cod_rule, return_value=False):

	conn = getDatabaseConnection(DB_CONF_FILE)
	cur = conn.cursor()

	try:
		cur.execute(sql_script)
	except Exception as e:
		conn.rollback() 
		scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
		scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3, cod_rule)		# SCAI
		print (e)
		print (e.pgerror)
		sys.exit("The process aborted with error when attempting to execute task calc.")
	else:
		conn.commit()

	if return_value:
		result = cur.fetchone()
		cur.close()
		conn.close()
		return result[0]
	else:	
		cur.close()
		conn.close()
	
	
	
#MAIN PROGRAM

DB_CONF_FILE = sys.argv[1] 	
CONF_FILE 	 = sys.argv[2] #When it is necessary to automate for more sites, add argument and below variables attribution, and add all cod_source_systems to variable so that it goes something like: COD_SOURCE_SYSTEM= (12,13,14)

data = json.load(open(CONF_FILE))
COD_COUNTRY 	  = data['COD_COUNTRY']
COD_INTEGRATION   = data['COD_INTEGRATION']
DSC_PROCESS		  = data['DSC_PROCESS']
COD_SOURCE_SYSTEM = data['COD_SOURCE_SYSTEM']
PATH_TO_SQL 	  = data['PATH_TO_SQL']

conn = getDatabaseConnection(DB_CONF_FILE)
cur = conn.cursor()



#SCAI Last run validation
scai_last_execution_status = scai.getLastExecutionStatus(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY)	# SCAI

if (scai_last_execution_status == 2):
	sys.exit("The integration is already running...")
	
if(scai_last_execution_status == 3):
	scai_process_status = scai.processCheck(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY,scai_last_execution_status)	# SCAI
		
# Is normal execution or re-execution starting from the step that was in error	
if (scai_last_execution_status == 1 or (scai_last_execution_status == 3 and scai_process_status == 3)):	
	#check in which block to begin
	cur.execute("select "\
				" nvl(block_nbr,1) as block_nbr "\
				" from crm_integration_anlt.t_rel_scai_country_integration country_integration "\
				" where "\
				"	country_integration.cod_integration = %(COD_INTEGRATION)d "\
				"	and country_integration.cod_country = %(COD_COUNTRY)d "\
				"	and ind_active = 1 "\
				% {
					'COD_COUNTRY':COD_COUNTRY ,
					'COD_INTEGRATION':COD_INTEGRATION
				}
			)
			
	conn.commit()
	
	results = cur.fetchone()
	
	#If above query does not return a value (For example on a normal execution, without previous errors)
	if (not results):
		block_nbr = 1
	else:
		block_nbr = results[0]
	print ("Starting from block: ", block_nbr)

	#Begin scai execution
	scai.integrationStart(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY)	# SCAI

	scai.processStart(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY) # SCAI

	try:
		cur.execute(
			" select cod_rule, "\
			" dsc_rule "\
			" from crm_integration_anlt.t_lkp_auto_task_rule "\
			" where 1 = 1 "\
			" and active = 1 "\
			" and cod_source_system in( ' " + COD_SOURCE_SYSTEM + "') "\
			" and cod_rule >= " + str(block_nbr) + " "\
			" order by 1 asc ")	
	except Exception as e: 
		scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
		scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3, block_nbr)		# SCAI
		print (e)
		print (e.pgerror)
		sys.exit("The process aborted with error when attempting to get sql script to execute.")

	result_list = cur.fetchall()
	 
	 
	for result in result_list: 

		cod_rule = result[0]
		sql_script = PATH_TO_SQL + result[1] + '.sql'
		print ("SQL script path: ", sql_script)
		try:
			sql_script = open(sql_script).read()
		except Exception as e: 
			scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
			scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3, block_nbr)		# SCAI
			print (e)
			print (e.pgerror)
			sys.exit("The process aborted with error when attempting to open sql script to execute.")
		print ("Executing Rule" , cod_rule, " at " , datetime.now().time())
		executeSQL(DB_CONF_FILE, sql_script, cod_rule)
		print ("Finished Executing Rule" , cod_rule, " at " , datetime.now().time())
		block_nbr = block_nbr + 1



	
	
scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '', 1) # SCAI

scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 1)		# SCAI
























