#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_storo_base_to_bd_contacts
import aut_storo_base_to_bd_deals
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
		deal.custom_fields['Lead Create Date'] = result[2] 			
		client.deals.update(deal.id, deal)

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database
	
#base_api_token = '81aef80f2a67ff2d70f0d905c15aa9fe5db3339f51a377370f585aa128ecc77f' #dev imopt

base_api_token = json.load(open(conf_file))['base_api_token_storo'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

print('Start Truncate aut_storo_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table sandbox_andre_matias.aut_storo_base_to_bd_contacts; ")
print('End Truncate aut_storo_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_storo_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table sandbox_andre_matias.aut_storo_base_to_bd_deals; ")
print('End Truncate aut_storo_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))

aut_storo_base_to_bd_contacts.main(conf_file)
aut_storo_base_to_bd_deals.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_storo_base_to_bd_contacts.contacts_id, "\
              "aut_storo_base_to_bd_contacts.deals_id, "\
              "aut_storo_base_to_bd_contacts.created_date "\
            "from "\
              "( "\
                "select "\
                  "aut_storo_base_to_bd_contacts.id contacts_id, "\
                  "aut_storo_base_to_bd_deals.id deals_id, "\
                  "to_char(to_date(left(aut_storo_base_to_bd_contacts.created_date,10),'yyyy-mm-dd'),'dd/mm/yyyy') as created_date "\
                "from "\
                  "sandbox_andre_matias.aut_storo_base_to_bd_contacts, "\
                  "sandbox_andre_matias.aut_storo_base_to_bd_deals "\
                "where "\
                  "aut_storo_base_to_bd_contacts.id = aut_storo_base_to_bd_deals.contacts_id "\
				  #"and aut_storo_base_to_bd_contacts.id = 200681893 "\
				  #"and aut_storo_base_to_bd_deals.id in(49245074,49245077) "\
              ") aut_storo_base_to_bd_contacts, "\
              "sandbox_andre_matias.aut_storo_base_to_bd_deals "\
            "where "\
              "aut_storo_base_to_bd_contacts.contacts_id = aut_storo_base_to_bd_deals.contacts_id (+) "\
              "and aut_storo_base_to_bd_contacts.deals_id = aut_storo_base_to_bd_deals.id (+) "\
              "and aut_storo_base_to_bd_contacts.created_date = aut_storo_base_to_bd_deals.created_date (+) "\
              "and aut_storo_base_to_bd_deals.id is null;")
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