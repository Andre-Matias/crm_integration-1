#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_atvro_base_to_bd_contacts
import aut_atvro_base_to_bd_deals
import aut_atvro_base_to_bd_base_user
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
		deal.name = result[2]		
		deal.custom_fields = Munch()
		deal.custom_fields['Main Category (*)'] = result[3] 		
		deal.custom_fields['Category listings (*)#2'] = result[4] 		
		deal.custom_fields['Type of business (*)'] = result[5] 		
		deal.custom_fields['State'] = result[6] 		
		deal.custom_fields['Contact owner'] = result[7] 	
		client.deals.update(deal.id, deal)

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database

base_api_token = json.load(open(conf_file))['base_api_token_atvro'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

print('Start Truncate aut_atvro_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table sandbox_andre_matias.aut_atvro_base_to_bd_contacts; ")
print('End Truncate aut_atvro_base_to_bd_contacts: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_atvro_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table sandbox_andre_matias.aut_atvro_base_to_bd_deals; ")
print('End Truncate aut_atvro_base_to_bd_deals: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_atvro_base_to_bd_base_user: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table sandbox_andre_matias.aut_atvro_base_to_bd_base_user; ")
print('End Truncate aut_atvro_base_to_bd_base_user: ' + time.strftime("%H:%M:%S"))

aut_atvro_base_to_bd_contacts.main(conf_file)
aut_atvro_base_to_bd_deals.main(conf_file)
aut_atvro_base_to_bd_base_user.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_atvro_base_to_bd_contacts.contacts_id, "\
              "aut_atvro_base_to_bd_contacts.deals_id, "\
              "aut_atvro_base_to_bd_contacts.name, "\
              "aut_atvro_base_to_bd_contacts.main_category, "\
              "aut_atvro_base_to_bd_contacts.category_listings, "\
              "aut_atvro_base_to_bd_contacts.type_of_business, "\
              "aut_atvro_base_to_bd_contacts.state, "\
              "aut_atvro_base_to_bd_contacts.owner "\
            "from "\
              "( "\
                "select "\
                  "aut_atvro_base_to_bd_contacts.id contacts_id, "\
                  "aut_atvro_base_to_bd_deals.id deals_id, "\
                  "replace(aut_atvro_base_to_bd_contacts.name + ' ' + aut_atvro_base_to_bd_deals.deal_objective + ' ' + aut_atvro_base_to_bd_deals.value, '  ',' ') as name, "\
				  "aut_atvro_base_to_bd_contacts.main_category, "\
				  "replace(replace(replace(replace(aut_atvro_base_to_bd_contacts.category_listings,'\"',''),'[',''),']',''),',',', ') as category_listings, "\
				  "aut_atvro_base_to_bd_contacts.type_of_business, "\
				  "contacts_state.state, "\
				  "aut_atvro_base_to_bd_base_user.name as owner "\
                "from "\
                  "sandbox_andre_matias.aut_atvro_base_to_bd_contacts, "\
                  "sandbox_andre_matias.aut_atvro_base_to_bd_deals, "\
				  "( "\
					"select id, replace(replace(replace(listagg(address_value, ', ') within group (order by id, cod_address_name),', null',''),'null, ',''),'null',' ') state "\
					"from ( "\
					"select "\
					  "id, "\
					  "state, "\
					  "case when (case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end) = 'line1' then 1 "\
					  "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end) = 'postal_code' then 2 "\
					  "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end) = 'state' then 3 "\
					  "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end) = 'city' then 4 "\
					  "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end) = 'country' then 5 "\
					  "end cod_address_name, "\
					  "case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',1),'{\"',''),'\"}','') end address_name, "\
					  "case when segment = '{}' then null else replace(replace(split_part(segment,'\":\"',2),'{\"',''),'\"}','') end address_value "\
					"from "\
					  "( "\
						"select "\
						  "ts.id, "\
						  "ts.state, "\
						  "s.gen_num, "\
						  "split_part(replace(replace(replace(replace(replace(state,':null',':\"null\"'),':false,',':\"false\",'),':true,',':\"true\",'),':false}',':\"false\"}'),':true}',':\"true\"}'),'\",\"', s.gen_num) AS segment "\
						"from "\
						  "sandbox_andre_matias.aut_atvro_base_to_bd_contacts ts, "\
						  "( "\
						  "select "\
							"* "\
						  "from "\
							"( "\
							  "select (1000 * t1.num) + (100 * t2.num) + (10 * t3.num) + t4.num AS gen_num "\
							  "from "\
							  "(select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t1, "\
							  "(select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t2, "\
							  "(select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t3, "\
							  "(select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t4  "\
							") "\
						  "where "\
						  "gen_num between 1 and (select max(regexp_count(replace(state,':null',':\"null\"'), '\\\",\"') + 1) from sandbox_andre_matias.aut_atvro_base_to_bd_contacts /*where id = 117058292*/) "\
						  ") s "\
						"where "\
						  "split_part(replace(state,':null',':\"null\"'), '\",\"', s.gen_num) != '' "\
						  "and state != '{}' "\
					  ") "\
					"order by 1,3) "\
					  "where cod_address_name = 3 "\
					"group by id "\
				  ") contacts_state, "\
				  "sandbox_andre_matias.aut_atvro_base_to_bd_base_user "\
                "where "\
                  "aut_atvro_base_to_bd_contacts.id = aut_atvro_base_to_bd_deals.contacts_id "\
                  "and aut_atvro_base_to_bd_contacts.id = contacts_state.id (+) "\
				  "and aut_atvro_base_to_bd_contacts.owner = aut_atvro_base_to_bd_base_user.id (+) "\
				  #"and aut_atvro_base_to_bd_contacts.id = 200682447 "\
				  #"and aut_atvro_base_to_bd_deals.id in(49245172,49245175) "\
              ") aut_atvro_base_to_bd_contacts, "\
              "sandbox_andre_matias.aut_atvro_base_to_bd_deals "\
            "where "\
              "aut_atvro_base_to_bd_contacts.contacts_id = aut_atvro_base_to_bd_deals.contacts_id (+) "\
              "and aut_atvro_base_to_bd_contacts.deals_id = aut_atvro_base_to_bd_deals.id (+) "\
              "and aut_atvro_base_to_bd_contacts.name = aut_atvro_base_to_bd_deals.name (+) "\
              "and aut_atvro_base_to_bd_contacts.main_category = aut_atvro_base_to_bd_deals.main_category (+) "\
              "and aut_atvro_base_to_bd_contacts.category_listings = aut_atvro_base_to_bd_deals.category_listings (+) "\
              "and aut_atvro_base_to_bd_contacts.type_of_business = aut_atvro_base_to_bd_deals.type_of_business (+) "\
              "and aut_atvro_base_to_bd_contacts.state = aut_atvro_base_to_bd_deals.state (+) "\
              "and aut_atvro_base_to_bd_contacts.owner = aut_atvro_base_to_bd_deals.owner (+) "\
              "and aut_atvro_base_to_bd_deals.id is null;")
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