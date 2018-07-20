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
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai


MAX_ERRORS_SKIPPED = 5

		
def getTask(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, base_api_token, cod_task = 999999999):

	client = basecrm.Client(access_token=base_api_token)
	 
	number_of_errors = 0
	keep_trying = True
	while keep_trying:
		try:
			if(number_of_errors > MAX_ERRORS_SKIPPED): 
				return -2
			getBaseTask = client.tasks.retrieve(id = cod_task)
			break
		except basecrm.errors.ServerError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.ServerError", str(err))
			print("Error: basecrm.errors.ServerError\nDescription: " + str(err) + "\nTrying again...")
			number_of_errors = number_of_errors + 1
		except basecrm.errors.RateLimitError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.RateLimitError", str(err))
			print("Error: basecrm.errors.RateLimitError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except requests.exceptions.ConnectionError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "requests.exceptions.ConnectionError", str(err))
			print("Error: requests.exceptions.ConnectionError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except basecrm.errors.RequestError as err:	
			for error in err.errors:
				#print (error.code) 
				if (error.code == "not_found"): 
					return -1
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.ServerError", str(err))
			print("Error: basecrm.errors.ServerError\nDescription: " + str(err) + "\nTrying again...")
			number_of_errors = number_of_errors + 1
		except Exception as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "Exception with general handling", str(err))
			print("Error\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)	
		
	return getBaseTask
 


def createTask(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, base_api_token, content, resource_type, due_date = '2099-12-31 00:00:00', owner_id = 1, resource_id = 999999999, completed = False, remind_at = '2099-12-31 00:00:00'):

	client = basecrm.Client(access_token=base_api_token)
	
	number_of_errors = 0
	keep_trying = True
	while keep_trying:
		try:
			if(number_of_errors > MAX_ERRORS_SKIPPED):
				return -2
			task = client.tasks.create(
			content 		= content,
			due_date 		= due_date,
			owner_id 		= owner_id,
			resource_type 	= resource_type,
			resource_id		= resource_id,
			completed		= completed, 
			remind_at 		= remind_at
			)  
			break
		except basecrm.errors.ServerError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.ServerError", str(err))
			print("Error: basecrm.errors.ServerError\nDescription: " + str(err) + "\nTrying again...")
			number_of_errors = number_of_errors + 1
		except basecrm.errors.RateLimitError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.RateLimitError", str(err))
			print("Error: basecrm.errors.RateLimitError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except requests.exceptions.ConnectionError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "requests.exceptions.ConnectionError", str(err))
			print("Error: requests.exceptions.ConnectionError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except Exception as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "Exception with general handling", str(err))
			print("Error\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)

	
	return task.id
	
	
	
def deleteTask(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, base_api_token, cod_task = 999999999):
	
	client = basecrm.Client(access_token=base_api_token)
	 
	number_of_errors = 0
	keep_trying = True
	while keep_trying:
		try:
			if(number_of_errors > MAX_ERRORS_SKIPPED): 
				return -2
			getBaseTask = client.tasks.destroy(id = cod_task)
			break
		except basecrm.errors.ServerError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.ServerError", str(err))
			print("Error: basecrm.errors.ServerError\nDescription: " + str(err) + "\nTrying again...")
			number_of_errors = number_of_errors + 1
		except basecrm.errors.RateLimitError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.RateLimitError", str(err))
			print("Error: basecrm.errors.RateLimitError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except requests.exceptions.ConnectionError as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "requests.exceptions.ConnectionError", str(err))
			print("Error: requests.exceptions.ConnectionError\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)
		except basecrm.errors.RequestError as err:	
			for error in err.errors:
				#print (error.code) 
				if (error.code == "not_found"): 
					return -1
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "basecrm.errors.ServerError", str(err))
			print("Error: basecrm.errors.ServerError\nDescription: " + str(err) + "\nTrying again...")
			number_of_errors = number_of_errors + 1
		except Exception as err:
			scai.logError(DB_CONF_FILE, DSC_PROCESS, COD_INTEGRATION, COD_COUNTRY, "Exception with general handling", str(err))
			print("Error\nDescription: " + str(err) + "\nTrying again in 1 second...")
			number_of_errors = number_of_errors + 1; time.sleep(1)	
		
	return 1	
	
	



