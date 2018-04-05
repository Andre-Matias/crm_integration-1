import csv
import sys, os
import ast
import psycopg2
import json
import basecrm
import gzip
import threading
import queue
from retry import retry
import logging
from datetime import date, datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))  # Change this later to a package import
import scai


logging.basicConfig()
logger = logging.getLogger('logger')

COD_INTEGRATION = 50000		# Analytical to Base
COD_COUNTRY = -1			# Replaced by code in conf_file
CONTACT_ID_IDX = 0
CUSTOM_FIELD_NAME_IDX = 1
CUSTOM_FIELD_VALUE_IDX = 2
MAX_ACTIVE_THREADS = 5


def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])


@retry(exceptions=Exception, delay=1, tries=10, logger=logger)	
def updateContactsInBase(client, contact_list, return_queue):
	number_of_updates = 0

	for contact in contact_list:
		number_of_updates = number_of_updates + 1
		client.contacts.update(contact.id, contact)
	
	print('Thread done sending contacts to Base!')
		
	return_queue.put(number_of_updates)


def main(db_conf_file, conf_file):
	print(datetime.now().time())

	data = json.load(open(conf_file))
	cod_source_system = data['cod_source_system']
	base_api_token = data['base_api_token']
	dsc_process = data['dsc_process']
	
	global COD_COUNTRY; COD_COUNTRY = int(data['cod_country'])
	
	# Create Redshift Connection
	print('Connecting to Database...')
	conn = getDatabaseConnection(db_conf_file)
	cur = conn.cursor()

	scai.processStart(db_conf_file, dsc_process, COD_INTEGRATION, COD_COUNTRY) # SCAI

	# Obtain the list of custom fields and contacts to update in Base; This is a list of tuples (opr_contact, dsc_custom_field, custom_field_value)
	print('Querying for contacts with custom fields to update to Base with cod_source_system ' + cod_source_system + '...')
	# TODO: Confirm dsc_process_short name
	cur.execute(
		"SELECT contact.opr_contact, "\
		"  custom_field.dsc_custom_field, "\
		"  fac.custom_field_value "\
		"FROM crm_integration_anlt.t_fac_base_integration_snap fac, "\
		"crm_integration_anlt.t_lkp_contact contact, "\
		"crm_integration_anlt.t_lkp_custom_field custom_field, "\
		"crm_integration_anlt.t_rel_scai_integration_process rel, "\
		"crm_integration_anlt.t_lkp_scai_process process "\
		"WHERE fac.cod_custom_field = custom_field.cod_custom_field "\
		"AND fac.cod_contact = contact.cod_contact "\
		"AND fac.dat_snap = rel.dat_processing "\
		"AND rel.cod_process = process.cod_process "\
		"AND process.dsc_process_short = '" + dsc_process + "' "\
		"AND fac.cod_source_system = " + cod_source_system + " "\
		"AND contact.valid_to = 20991231;")	
	print('Extracting query results...')
	result_list = cur.fetchall()
	#print('Results:')
	#print(result_list)

	print('Closing Database connection...')

	cur.close()
	conn.close()
	
	print(datetime.now().time())

	# Create Base Connection
	print('Connecting to ' + dsc_process + '...')
	client = basecrm.Client(access_token=base_api_token)
	
	# Put all query results in a dictionary with key as opr_contact, and value as a list of the tuples the query returned (one for each custom field of that contact)
	result_dictionary = dict()
	for result in result_list:
		if result[CONTACT_ID_IDX] in result_dictionary:
			result_dictionary[result[CONTACT_ID_IDX]].append(result)
		else:
			result_dictionary[result[CONTACT_ID_IDX]] = [result]

	# Get contacts and iterate through them
	contact_dictionary = dict()
	page_nbr = 1
	contacts_data = '1'
	number_of_updates = 0
	
	while len(contacts_data) > 0:
		print('Page #' + str(page_nbr))
		contacts_data = client.contacts.list(page=page_nbr, per_page=100)
		
		# Code could be further improved if all contacts are acquired from Base first, put in a dictionary, and then do updates later (will use much more memory, however)
		# Alternatively, a dictionary with 100 contacts could be created for every iteration here, instead of putting all contacts in a single dictionary; there could still be some time gains compared to current implementation
		for contact in contacts_data:
			if contact.id in result_dictionary:
				for result in result_dictionary[contact.id]:
					contact.custom_fields[result[CUSTOM_FIELD_NAME_IDX]] = result[CUSTOM_FIELD_VALUE_IDX]
					number_of_updates = number_of_updates + 1
				contact_dictionary[contact.id] = contact
							
		page_nbr = page_nbr + 1

	print(datetime.now().time())	
	print('Number of updates done in code: ' + str(number_of_updates))
	
	# Update contacts in Base
	print('Updating #' + str(len(contact_dictionary)) + ' contacts in Base')
	
	#input('Ready to send contacts to Base. Proceed?')
	
	# Threading implementation
	number_active_threads = 0
	contact_list = list(contact_dictionary.values())
	number_contacts = len(contact_list)
	contacts_per_thread = - (-number_contacts // MAX_ACTIVE_THREADS) # Ceiling of integer division
	thread_list = []
	thread_return_values_queue = queue.Queue()
	
	i = 0
	j = contacts_per_thread
	for n in range(0, MAX_ACTIVE_THREADS):
		t = threading.Thread(target=updateContactsInBase, args=(client, contact_list[i:j], thread_return_values_queue))
		thread_list.append(t)
		t.start()
		print('Spawned thread #' + str(n+1))
		i = i + contacts_per_thread
		j = j + contacts_per_thread
		if j > number_contacts:
			j = number_contacts
	
	for t in thread_list:
		t.join()
	
	number_of_updates = 0
	while not thread_return_values_queue.empty():
		number_of_updates = number_of_updates + thread_return_values_queue.get()
	
	print('Number of updates done in ' + dsc_process + ': ' + str(number_of_updates))
	scai.processEnd(db_conf_file, dsc_process, COD_INTEGRATION, COD_COUNTRY) # SCAI
	print(datetime.now().time())
	print('Done\n')

	
# Test if this is being run as a standalone program and not an utility module
if __name__ == "__main__":
	db_conf_file = sys.argv[1]	# Database configuration file
	conf_file = sys.argv[2] 	# Base configuration file
	
	main(db_conf_file, conf_file)