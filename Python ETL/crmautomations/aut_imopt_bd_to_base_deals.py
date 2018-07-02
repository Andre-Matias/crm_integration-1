#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys, os
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_imopt_base_to_bd_contacts
import aut_imopt_base_to_bd_deals
import threading
from retry import retry
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
import scai

logging.basicConfig()
logger = logging.getLogger('logger')
MAX_ACTIVE_THREADS = 5

COD_INTEGRATION = 60000					# Chandra to Operational
COD_COUNTRY = -1						# Replaced by code in conf_file
scai_process_name = "aut_imopt_deals_insert_to_base"

@retry(exceptions=Exception, delay=1, tries=10, logger=logger)	
def updateDealsInBase(client, result_list):
	#Update dos Deals
	for result in result_list:
		deal = Munch()
		deal.id = result[1]
		deal.custom_fields = Munch()
		deal.custom_fields['NIF'] = result[2]	
		deal.custom_fields['User ID'] = result[3] 	
		deal.custom_fields['Designação Fiscal'] = result[4] 	
		deal.custom_fields['Fiscal Address'] = result[5] 	
		deal.custom_fields['Email Address'] = result[6] 	
		deal.custom_fields['Natureza'] = result[7] 	
		client.deals.update(deal.id, deal)

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database
COD_COUNTRY = int(sys.argv[2])  # Country code

base_api_token = json.load(open(conf_file))['base_api_token_imopt'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

scai_last_execution_status = scai.getLastExecutionStatus(conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

if (scai_last_execution_status == 2):
	sys.exit("The integration is already running...")
	
scai.integrationStart(conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI	
scai.processStart(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI

print('Start Truncate aut_imopt_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_imopt_base_to_bd_contact; ")
print('End Truncate aut_imopt_base_to_bd_contact: ' + time.strftime("%H:%M:%S"))

print('Start Truncate aut_imopt_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))
cur.execute("truncate table crm_integration_anlt.aut_imopt_base_to_bd_deal; ")
print('End Truncate aut_imopt_base_to_bd_deal: ' + time.strftime("%H:%M:%S"))

aut_imopt_base_to_bd_contacts.main(conf_file)
aut_imopt_base_to_bd_deals.main(conf_file)

print('Starting Query: ' + time.strftime("%H:%M:%S"))

#Contact Differences
cur.execute(
			"select "\
              "aut_imopt_base_to_bd_contact.contacts_id, "\
              "aut_imopt_base_to_bd_contact.deals_id, "\
              "aut_imopt_base_to_bd_contact.nif, "\
              "aut_imopt_base_to_bd_contact.user_id, "\
              "aut_imopt_base_to_bd_contact.designacao_fiscal, "\
              "aut_imopt_base_to_bd_contact.fiscal_address, "\
              "aut_imopt_base_to_bd_contact.email_address, "\
              "aut_imopt_base_to_bd_contact.natureza "\
            "from "\
              "( "\
                "select "\
                  "aut_imopt_base_to_bd_contact.id contacts_id, "\
                  "aut_imopt_base_to_bd_deal.id deals_id, "\
                  "aut_imopt_base_to_bd_contact.nif, "\
                  "aut_imopt_base_to_bd_contact.user_id, "\
                  "aut_imopt_base_to_bd_contact.designacao_fiscal, "\
                  "contacts_address.fiscal_address, "\
                  "aut_imopt_base_to_bd_contact.email_address, "\
                  "replace(replace(replace(aut_imopt_base_to_bd_contact.natureza,'\\\"',''),'[',''),']','') as natureza "\
                "from "\
                  "crm_integration_anlt.aut_imopt_base_to_bd_contact, "\
                  "crm_integration_anlt.aut_imopt_base_to_bd_deal, "\
                  "( "\
                    "select id, replace(replace(replace(listagg(address_value, ', ') within group (order by id, cod_address_name),', null',''),'null, ',''),'null',' ') fiscal_address "\
                    "from ( "\
                    "select "\
                      "id, "\
                      "fiscal_address, "\
                      "case when (case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end) = 'line1' then 1 "\
                      "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end) = 'postal_code' then 2 "\
                      "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end) = 'state' then 3 "\
                      "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end) = 'city' then 4 "\
                      "when (case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end) = 'country' then 5 "\
                      "end cod_address_name, "\
                      "case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',1),'{\\\"',''),'\\\"}','') end address_name, "\
                      "case when segment = '{}' then null else replace(replace(split_part(segment,'\\\":\\\"',2),'{\\\"',''),'\\\"}','') end address_value "\
                    "from "\
                      "( "\
                        "select "\
                          "ts.id, "\
                          "ts.fiscal_address, "\
                          "s.gen_num, "\
                          "split_part(replace(replace(replace(replace(replace(fiscal_address,':null',':\\\"null\\\"'),':false,',':\\\"false\\\",'),':true,',':\\\"true\\\",'),':false}',':\\\"false\\\"}'),':true}',':\\\"true\\\"}'),'\\\",\\\"', s.gen_num) AS segment "\
                        "from "\
                          "crm_integration_anlt.aut_imopt_base_to_bd_contact ts, "\
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
                              "(select 1 as num union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 0) t4 "\
                            ") "\
                          "where "\
                          "gen_num between 1 and (select max(regexp_count(replace(fiscal_address,':null',':\\\"null\\\"'), '\\\",\\\"') + 1) from crm_integration_anlt.aut_imopt_base_to_bd_contact /*where id = 117058292*/) "\
                          ") s "\
                        "where "\
                          "split_part(replace(fiscal_address,':null',':\\\"null\\\"'), '\\\",\\\"', s.gen_num) != '' "\
                          "and fiscal_address != '{}' "\
                      ") "\
                    "order by 1,3) "\
                    "group by id "\
                    ") contacts_address "\
                "where  "\
                  "aut_imopt_base_to_bd_contact.id = aut_imopt_base_to_bd_deal.contacts_id "\
                "and aut_imopt_base_to_bd_contact.id = contacts_address.id (+) "\
				#"and aut_imopt_base_to_bd_contact.id = 200682693 "\
				#"and aut_imopt_base_to_bd_deal.id in(49245232,49245233) "\
              ") aut_imopt_base_to_bd_contact, "\
              "crm_integration_anlt.aut_imopt_base_to_bd_deal "\
            "where "\
              "aut_imopt_base_to_bd_contact.contacts_id = aut_imopt_base_to_bd_deal.contacts_id (+) "\
              "and aut_imopt_base_to_bd_contact.deals_id = aut_imopt_base_to_bd_deal.id (+) "\
              "and aut_imopt_base_to_bd_contact.nif = aut_imopt_base_to_bd_deal.nif (+) "\
              "and aut_imopt_base_to_bd_contact.user_id = aut_imopt_base_to_bd_deal.user_id (+) "\
              "and aut_imopt_base_to_bd_contact.designacao_fiscal = aut_imopt_base_to_bd_deal.designacao_fiscal (+) "\
              "and aut_imopt_base_to_bd_contact.fiscal_address = aut_imopt_base_to_bd_deal.fiscal_address (+) "\
              "and aut_imopt_base_to_bd_contact.email_address = aut_imopt_base_to_bd_deal.email_address (+) "\
              "and aut_imopt_base_to_bd_contact.natureza = aut_imopt_base_to_bd_deal.natureza (+) "\
              "and aut_imopt_base_to_bd_deal.id is null;")
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