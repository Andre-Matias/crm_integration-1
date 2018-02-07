import csv
import sys
import ast
import psycopg2
import json
import basecrm
import gzip
from datetime import date, datetime

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

print(datetime.now().time())
	
source_conf_file = sys.argv[1] # File with source database
CONTACT_ID_IDX = 0
CUSTOM_FIELD_NAME_IDX = 1
CUSTOM_FIELD_VALUE_IDX = 2
	
# Create Base Connection
print('Connecting to Base...')
#base_api_token = 'dbba48886bd80475d1e5866ab0aff777c3f100635626c95f4d4fef7d41bd4bd7'	# Sandbox
#base_api_token = '91304eb568e4a46333f0e93b355b1f83d638e700120aec621a5e8b8a5c766404'	# Real deal
base_api_token = 'ea0efe0628f3f5d4826eb2152cddd37bcc2c214cf9098c6069151fda5be157aa'		# Real deal 2 (testing if fewer API privileges means better performance)
client = basecrm.Client(access_token=base_api_token)

# Create Redshift Connection
print('Connecting to Chandra...')
conn = getDatabaseConnection(source_conf_file)
cur = conn.cursor()

# Obtain the list of custom fields and contacts to update in Base; This is a list of tuples (opr_contact, dsc_custom_field, custom_field_value)
print('Querying for contacts with custom fields to update to Base...')
# TODO: Confirm dsc_process_short name
cur.execute(
	"SELECT contact.opr_contact, "\
	"  custom_field.dsc_custom_field, "\
	"  fac.custom_field_value "\
	"FROM sandbox_andre_matias.t_fac_base_integration_snap fac, "\
	"sandbox_andre_matias.t_lkp_contact contact, "\
	"sandbox_andre_matias.t_lkp_custom_field custom_field, "\
	"sandbox_andre_matias.t_rel_scai_integration_process rel, "\
	"sandbox_andre_matias.t_lkp_scai_process process "\
	"WHERE fac.cod_custom_field = custom_field.cod_custom_field "\
	"AND fac.cod_contact = contact.cod_contact "\
	"AND fac.dat_snap = rel.dat_processing "\
	"AND rel.cod_process = process.cod_process "\
	"AND process.dsc_process_short = 't_fac_base_integration_snap' "\
	"AND contact.valid_to = 20991231;")	
print('Extracting query results...')
result_list = cur.fetchall()
#print('Results:')
#print(result_list)

print('Closing Database connection...')

cur.close()
conn.close()

print(datetime.now().time())

# Get contacts and iterate through them
contact_dictionary = dict()
page_nbr = 1
contacts_data = '1'
number_of_updates = 0
while len(contacts_data) > 0:
	print('Page #' + str(page_nbr))
	contacts_data = client.contacts.list(page=page_nbr, per_page=100)
	#print('# Contacts: ' + str(len(contacts_data)))

	for contact in contacts_data:
		for result in result_list:
			if(contact.id == result[CONTACT_ID_IDX]):
				number_of_updates = number_of_updates + 1
				contact.custom_fields[result[CUSTOM_FIELD_NAME_IDX]] = result[CUSTOM_FIELD_VALUE_IDX]
				contact_dictionary[contact.id] = contact
				# This assignment is being done many times for the same contact, if there's more than 1 KPI for this contact; should check at the end to see if there was any change, so it could be assigned only once

	page_nbr = page_nbr + 1

print(datetime.now().time())	
print('Number of updates done in code: ' + str(number_of_updates))
number_of_updates = 0

# Update contacts in Base
print('Updating #' + str(len(contact_dictionary)) + ' contacts in Base')
#input('Ready to update contacts in base?\n')
for contact in contact_dictionary.itervalues():
	client.contacts.update(contact.id, contact)
	number_of_updates = number_of_updates + 1

print('Number of updates done in Base: ' + str(number_of_updates))
print(datetime.now().time())

print('Done\n')