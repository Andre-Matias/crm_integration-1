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

def updateFacAutoTask(cod_auto_task, cod_status, message, opr_task = 0):
	try:
		cur.execute(
		" update "\
		"	crm_integration_anlt.t_fac_auto_task "\
		"	set cod_status = " + cod_status + ", "\
		"   dat_task_sync = cast(to_char(sysdate,'YYYYMMDD') as integer), "\
		"   opr_task = case when '" + str(opr_task) + "' = 0 then opr_task else '" + str(opr_task) + "' end "\
		"	where cod_auto_task = " + str(cod_auto_task) + " ")	
	except Exception as e: 
		scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
		scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
		print (e)
		print (e.pgerror)
		conn.rollback()
		sys.exit(message)
	
	conn.commit()
	

DB_CONF_FILE = sys.argv[1] 	
CONF_FILE 	 = sys.argv[2]

data = json.load(open(CONF_FILE))
BASE_APPLICATON	  = data['BASE_APPLICATON']
COD_COUNTRY 	  = data['COD_COUNTRY']
COD_INTEGRATION   = data['COD_INTEGRATION']
DSC_PROCESS		  = data['DSC_PROCESS']
COD_SOURCE_SYSTEM = data['COD_SOURCE_SYSTEM']

conn = getDatabaseConnection(DB_CONF_FILE)
cur = conn.cursor()

#Begin scai execution
scai.integrationStart(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY)	# SCAI

scai.processStart(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY) # SCAI

try:
	cur.execute(
		" select token "\
		" from crm_integration_anlt.t_lkp_token "\
		" where 1 = 1 "\
		" and application = '" + BASE_APPLICATON + "' ")	
except Exception as e: 
	scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
	scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
	print (e)
	print (e.pgerror)
	sys.exit("The process aborted with error when attempting to get token.")

BASE_TOKEN = cur.fetchone()[0]
#print (BASE_TOKEN)
 

try:
	cur.execute(
		" select "\
		"	task.cod_auto_task, "\
		"	task.content || '(Auto_task_' || task.cod_auto_task || ')' as content, "\
		"	task.resource_type, "\
		"	task.due_date, "\
		"	task.owner_id, "\
		"	task.resource_id, "\
		"	task.completed, "\
		"	task.remind_at "\
		"	from crm_integration_anlt.t_fac_auto_task task "\
		"	where 1 = 1 "\
		"	and task.cod_source_system = " + COD_SOURCE_SYSTEM + " "\
		"	and task.cod_status = 3 "\
		"	order by 1 asc ")	
except Exception as e: 
	scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '',3)	# SCAI
	scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 3)		# SCAI
	print (e)
	print (e.pgerror)
	sys.exit("The process aborted with error when attempting to get list of tasks.")

result_list = cur.fetchall()


for result in result_list:

	cod_auto_task   = result[0]
	content			= result[1]
	resource_type 	= result[2]
	due_date 		= result[3]
	owner_id 		= result[4] 
	resource_id 	= result[5]
	completed 		= result[6]
	remind_at 		= result[7]
	
	print ("Running task: ", cod_auto_task)

	
	result_task_create = base_auto_task.createTask(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, BASE_TOKEN, content, resource_type, due_date, owner_id, resource_id, completed, remind_at)
	print (result_task_create)

	if (result_task_create == -2):
		print ('An error occurred when attempting to create the task.')
		#Log the error on DB table
		updateFacAutoTask(cod_auto_task, "4", "The process aborted with error when attempting to save error on task creation.")
	else:
		print ('Task ' , result_task_create, ' created successfuly.')
		#Log success on DB table
		updateFacAutoTask(cod_auto_task, "2", "The process aborted with error when attempting to save success on task creation.", result_task_create)
			
		#verify if created
		result_task_get = base_auto_task.getTask(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, BASE_TOKEN, cod_task = result_task_create)

		if (result_task_get == -2):
			print ('An error occurred when attempting to verify the task.')
			#Log the error on DB table
			updateFacAutoTask(cod_auto_task, "4","The process aborted with error when attempting to save error on task verification.")
		elif (result_task_get == -1):
			print ('Task not found.')
			#Log not verified on DB table
			updateFacAutoTask(cod_auto_task, "5", "The process aborted with error when attempting to save error on task verification.")
		else: 
			if (result_task_get.id == result_task_create and str(result_task_get.content[result_task_get.content.find('(Auto_task_')+11:-1]) == str(cod_auto_task)):
				print ('Task verified successfuly.')
				#Log that task verified successfuly on DB table
				updateFacAutoTask(cod_auto_task, "1", "The process aborted with error when attempting to save success on task verification.")
			else:
				print ('Task not verified successfuly.')
				#Log that task not verified successfuly on DB table
				updateFacAutoTask(cod_auto_task, "5", "The process aborted with error when attempting to save success on task verification.")
	
	
	
	
scai.processEnd(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, '', '', 1) # SCAI

scai.integrationEnd(DB_CONF_FILE, COD_INTEGRATION, COD_COUNTRY, 1)		# SCAI



















