#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_olxpt_base_to_bd_contacts_full
import aut_olxpt_base_to_bd_deals_full
import threading
from retry import retry
import logging

logging.basicConfig()
logger = logging.getLogger('logger')
MAX_ACTIVE_THREADS = 5

@retry(exceptions=Exception, delay=1, tries=10, logger=logger)	
def updateDealsInBase(client, result_list):
	#Update dos Deals
	for result in result_list:
		deal = Munch()
		deal.id = result[1]
		deal.custom_fields = Munch()
		deal.custom_fields['Categoria'] = result[2]	
		deal.custom_fields['Email'] = result[3] 	
		deal.custom_fields['Mobile'] = result[4] 	
		deal.custom_fields['NIF'] = result[5] 	
		deal.custom_fields['Phone'] = result[6] 	
		deal.custom_fields['User ID'] = result[7] 	
		client.deals.update(deal.id, deal)
	
def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database

base_api_token = json.load(open(conf_file))['base_api_token_olxpt'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

print('Start Truncate aut_olxpt_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_olxpt_base_to_bd_contacts; ")
print('End Truncate aut_olxpt_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_olxpt_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_olxpt_base_to_bd_deals; ")
print('End Truncate aut_olxpt_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))

aut_olxpt_base_to_bd_contacts.main(conf_file) 
aut_olxpt_base_to_bd_deals.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_olxpt_base_to_bd_contacts.contacts_id, "\
              "aut_olxpt_base_to_bd_contacts.deals_id, "\
              "aut_olxpt_base_to_bd_contacts.categoria, "\
              "aut_olxpt_base_to_bd_contacts.email, "\
              "aut_olxpt_base_to_bd_contacts.mobile, "\
              "aut_olxpt_base_to_bd_contacts.nif, "\
              "aut_olxpt_base_to_bd_contacts.phone, "\
              "aut_olxpt_base_to_bd_contacts.user_id "\
            "from "\
              "( "\
                "select "\
                  "aut_olxpt_base_to_bd_contacts.id contacts_id, "\
                  "aut_olxpt_base_to_bd_deals.id deals_id, "\
                  "aut_olxpt_base_to_bd_contacts.categoria, "\
                  "aut_olxpt_base_to_bd_contacts.email, "\
                  "aut_olxpt_base_to_bd_contacts.mobile, "\
                  "aut_olxpt_base_to_bd_contacts.nif, "\
                  "aut_olxpt_base_to_bd_contacts.phone, "\
                  "aut_olxpt_base_to_bd_contacts.user_id "\
                "from "\
                  "crm_integration_anlt.aut_olxpt_base_to_bd_contacts, "\
                  "crm_integration_anlt.aut_olxpt_base_to_bd_deals "\
                "where "\
                  "aut_olxpt_base_to_bd_contacts.id = aut_olxpt_base_to_bd_deals.contacts_id "\
				  #"and aut_olxpt_base_to_bd_contacts.id = 200682721 "\
				  #"and aut_olxpt_base_to_bd_deals.id in(49245239, 49245240) "\
              ") aut_olxpt_base_to_bd_contacts, "\
              "crm_integration_anlt.aut_olxpt_base_to_bd_deals "\
            "where "\
              "aut_olxpt_base_to_bd_contacts.contacts_id = aut_olxpt_base_to_bd_deals.contacts_id (+) "\
              "and aut_olxpt_base_to_bd_contacts.deals_id = aut_olxpt_base_to_bd_deals.id (+) "\
              "and aut_olxpt_base_to_bd_contacts.categoria = aut_olxpt_base_to_bd_deals.categoria (+) "\
              "and aut_olxpt_base_to_bd_contacts.email = aut_olxpt_base_to_bd_deals.email (+) "\
              "and aut_olxpt_base_to_bd_contacts.mobile = aut_olxpt_base_to_bd_deals.mobile (+) "\
              "and aut_olxpt_base_to_bd_contacts.nif = aut_olxpt_base_to_bd_deals.nif (+) "\
              "and aut_olxpt_base_to_bd_contacts.phone = aut_olxpt_base_to_bd_deals.phone (+) "\
              "and aut_olxpt_base_to_bd_contacts.user_id = aut_olxpt_base_to_bd_deals.user_id (+) "\
              "and aut_olxpt_base_to_bd_deals.id is null;")
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

cur.close()
conn.close()