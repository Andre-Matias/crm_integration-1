#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_stvpt_base_to_bd_contacts_full
import aut_stvpt_base_to_bd_deals_full
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
		deal.custom_fields['NIF'] = result[2]		
		deal.custom_fields['User_ID'] = result[3] 	
		client.deals.update(deal.id, deal)

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database

base_api_token = json.load(open(conf_file))['base_api_token_stvpt'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

print('Start Truncate aut_stvpt_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_stvpt_base_to_bd_contact; ")
print('End Truncate aut_stvpt_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_stvpt_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_stvpt_base_to_bd_deal; ")
print('End Truncate aut_stvpt_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))

aut_stvpt_base_to_bd_contact.main(conf_file)
aut_stvpt_base_to_bd_deal.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_stvpt_base_to_bd_contact.contacts_id, "\
              "aut_stvpt_base_to_bd_contact.deals_id, "\
              "aut_stvpt_base_to_bd_contact.nif, "\
              "aut_stvpt_base_to_bd_contact.user_id "\
            "from "\
              "( "\
                "select "\
                  "aut_stvpt_base_to_bd_contact.id contacts_id, "\
                  "aut_stvpt_base_to_bd_deal.id deals_id, "\
                  "aut_stvpt_base_to_bd_contact.nif, "\
				  "aut_stvpt_base_to_bd_contact.user_id "\
                "from "\
                  "crm_integration_anlt.aut_stvpt_base_to_bd_contact, "\
                  "crm_integration_anlt.aut_stvpt_base_to_bd_deal "\
                "where "\
                  "aut_stvpt_base_to_bd_contact.id = aut_stvpt_base_to_bd_deal.contacts_id "\
				  #"and aut_stvpt_base_to_bd_contact.id = 200682665 "\
				  #"and aut_stvpt_base_to_bd_deal.id in(49245215,49245216) "\
              ") aut_stvpt_base_to_bd_contact, "\
              "crm_integration_anlt.aut_stvpt_base_to_bd_deal "\
            "where "\
              "aut_stvpt_base_to_bd_contact.contacts_id = aut_stvpt_base_to_bd_deal.contacts_id (+) "\
              "and aut_stvpt_base_to_bd_contact.deals_id = aut_stvpt_base_to_bd_deal.id (+) "\
              "and aut_stvpt_base_to_bd_contact.nif = aut_stvpt_base_to_bd_deal.nif (+) "\
              "and aut_stvpt_base_to_bd_contact.user_id = aut_stvpt_base_to_bd_deal.user_id (+) "\
              "and aut_stvpt_base_to_bd_deal.id is null;")
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