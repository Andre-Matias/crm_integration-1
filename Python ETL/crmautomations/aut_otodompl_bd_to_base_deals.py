#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys, os
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_otodompl_base_to_bd_contacts
import aut_otodompl_base_to_bd_deals
import threading
from retry import retry
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'crmintegration', '0_common'))  # Change this later to a package import
import scai

logging.basicConfig()
logger = logging.getLogger('logger')
MAX_ACTIVE_THREADS = 5

COD_INTEGRATION = 60000
COD_COUNTRY = -1						# Replaced by code in conf_file
scai_process_name = "aut_otodompl_deals_insert_to_base"

@retry(exceptions=Exception, delay=1, tries=10, logger=logger)	
def updateDealsInBase(client, result_list):
	#Update dos Deals
	for result in result_list:
		deal = Munch()
		deal.id = result[1]
		deal.custom_fields = Munch()
		deal.custom_fields['NIP'] = result[2]	
		deal.custom_fields['Customer Type'] = result[3] 	
		deal.custom_fields['Admin ID'] = result[4] 	
		deal.custom_fields['Numer telefonu'] = result[5]
		client.deals.update(deal.id, deal)

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database
COD_COUNTRY = int(sys.argv[2])  # Country code

base_api_token = json.load(open(conf_file))['base_api_token_otodompl']

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

scai_process_status = scai.processCheck(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, 1)	# SCAI

#First time ever execution
if (not scai_process_status):
	scai_process_status = 1

if (scai_process_status != 1):
	sys.exit("The integration is already running or there was an error with the last execution that has to be fixed manually.")

scai.integrationStart(conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI	
scai.processStart(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI

print('Start Truncate aut_otodompl_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_otodompl_base_to_bd_contact; ")
print('End Truncate aut_otodompl_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_otodompl_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_otodompl_base_to_bd_deal; ")
print('End Truncate aut_otodompl_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))

aut_otodompl_base_to_bd_contacts.main(conf_file)
aut_otodompl_base_to_bd_deals.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_otodompl_base_to_bd_contact.contact_id, "\
              "aut_otodompl_base_to_bd_contact.deal_id, "\
              "aut_otodompl_base_to_bd_contact.nip, "\
              "aut_otodompl_base_to_bd_contact.customer_type, "\
              "aut_otodompl_base_to_bd_contact.admin_id, "\
              "aut_otodompl_base_to_bd_contact.numer_telefonu "\
            "from "\
              "( "\
                "select "\
                  "aut_otodompl_base_to_bd_contact.id contact_id, "\
                  "aut_otodompl_base_to_bd_deal.id deal_id, "\
                  "aut_otodompl_base_to_bd_contact.nip, "\
                  "aut_otodompl_base_to_bd_contact.customer_type, "\
                  "aut_otodompl_base_to_bd_contact.admin_id, "\
                  "aut_otodompl_base_to_bd_contact.numer_telefonu "\
				"from "\
                  "crm_integration_anlt.aut_otodompl_base_to_bd_contact, "\
                  "crm_integration_anlt.aut_otodompl_base_to_bd_deal "\
                "where "\
                  "aut_otodompl_base_to_bd_contact.id = aut_otodompl_base_to_bd_deal.contact_id "\
              ") aut_otodompl_base_to_bd_contact, "\
              "crm_integration_anlt.aut_otodompl_base_to_bd_deal "\
            "where "\
              "aut_otodompl_base_to_bd_contact.contact_id = aut_otodompl_base_to_bd_deal.contact_id (+) "\
              "and aut_otodompl_base_to_bd_contact.deal_id = aut_otodompl_base_to_bd_deal.id (+) "\
              "and aut_otodompl_base_to_bd_contact.nip = aut_otodompl_base_to_bd_deal.nip (+) "\
              "and aut_otodompl_base_to_bd_contact.customer_type = aut_otodompl_base_to_bd_deal.customer_type (+) "\
              "and aut_otodompl_base_to_bd_contact.admin_id = aut_otodompl_base_to_bd_deal.admin_id (+) "\
              "and aut_otodompl_base_to_bd_contact.numer_telefonu = aut_otodompl_base_to_bd_deal.numer_telefonu (+) "\
              "and aut_otodompl_base_to_bd_deal.id is null;")
result_list = cur.fetchall()

print('End Query: ' + time.strftime("%H:%M:%S"))

print('Starting Updating: ' + time.strftime("%H:%M:%S"))

# Threading implementation
number_active_threads = 0
number_deals = len(result_list)
deals_per_thread = - (-number_deals // MAX_ACTIVE_THREADS) # Ceiling of integer division
thread_list = []

i = 0
j = deals_per_thread
for n in range(0, MAX_ACTIVE_THREADS):
	t = threading.Thread(target=updateDealsInBase, args=(client, result_list[i:j]))
	thread_list.append(t)
	t.start()
	print('Spawned thread #' + str(n+1))
	i = i + deals_per_thread
	j = j + deals_per_thread
	if j > number_deals:
		j = number_deals

for t in thread_list:
	t.join()

print('End of Updating: ' + time.strftime("%H:%M:%S"))

scai.processEnd(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, '', '',1)	# SCAI
scai.integrationEnd(conf_file, COD_INTEGRATION, COD_COUNTRY, 1)		# SCAI
cur.close()
conn.close()