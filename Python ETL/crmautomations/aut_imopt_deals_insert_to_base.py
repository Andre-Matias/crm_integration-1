#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys, os
import csv, ast, psycopg2, json, basecrm, gzip
import time
from retry import retry
import logging
import threading
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'crmintegration\\0_common'))  # Change this later to a package import
import scai

print('Starting Process... ' + time.strftime("%H:%M:%S"))

logging.basicConfig()
logger = logging.getLogger('logger')
MAX_ACTIVE_THREADS = 5
COD_INTEGRATION = 60000					# Chandra to Operational
COD_COUNTRY = -1						# Replaced by code in conf_file
scai_process_name = "aut_imopt_deals_creation"
	
@retry(exceptions=Exception, delay=1, tries=10, logger=logger)	
def createDealsInBase(client, result_list):
	#Update dos Deals
	for result in result_list:
		deal = client.deals.create(
				name=result[0],
				contact_id=result[1],
				value=result[2])
				#source_id= result[3]

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database
	
base_api_token = json.load(open(conf_file))['base_api_token_imopt'] 

client = basecrm.Client(access_token=base_api_token)

# Create Redshift Connection
conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

scai_last_execution_status = scai.getLastExecutionStatus(conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI

if (scai_last_execution_status != 1):
	sys.exit("The integration is already running or there was an error with the last execution that has to be fixed manually.")

	
scai.integrationStart(conf_file, COD_INTEGRATION, COD_COUNTRY)	# SCAI	
scai.processStart(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY)	# SCAI

print('Starting Data Query... ' + time.strftime("%H:%M:%S"))
cur.execute(
			"select "\
            	"fac.dsc_paidad_user_payment, "\
            	"lkp_contact.opr_contact, "\
            	"fac.val_price * (-1) as val_price "\
            	#"2467113 as Source /*'Organic'*/"\
            "from "\
            	"sandbox_andre_matias.t_fac_paidad_user_payment fac, "\
            	"sandbox_andre_matias.t_lkp_atlas_user lkp_atlas_user, "\
            	"sandbox_andre_matias.t_lkp_contact lkp_contact, "\
            	"sandbox_andre_matias.t_lkp_paidad_index lkp_pi "\
            "where "\
            	"fac.cod_atlas_user = lkp_atlas_user.cod_atlas_user "\
            	"and lkp_atlas_user.cod_source_system = 3 "\
            	"and lkp_contact.cod_source_system = 17 "\
            	"and lower(lkp_atlas_user.dsc_atlas_user) = lower(lkp_contact.email) "\
            	"and fac.cod_paidad_index = lkp_pi.cod_paidad_index "\
            	#"and lkp_pi.paidad_index_code not in('removebonus','removecredits', 'removerefunds') "\
				"and lkp_pi.flg_aut_deal_exclude != 1 "\
				"and lkp_pi.cod_source_system = 3 "\
            	"and val_price < 0 "\
				#"and fac.opr_paidad_user_payment = 6222281 "\
				"and dat_payment > (select last_processing_datetime from sandbox_andre_matias.aut_deals_insert_to_base_dates where source_system = 'ptre') "\
            	"and lkp_atlas_user.valid_to = 20991231 "\
            	"and lkp_contact.valid_to = 20991231 "\
            	"and lkp_pi.valid_to = 20991231; ")
result_list = cur.fetchall()
print('Ending Data Query... ' + time.strftime("%H:%M:%S"))

print('Starting Truncate Dates Query... ' + time.strftime("%H:%M:%S"))
cur.execute(
			"delete from sandbox_andre_matias.aut_deals_insert_to_base_dates where source_system = 'ptre'; ")
print('Ending Truncate Dates Query... ' + time.strftime("%H:%M:%S"))

print('Starting Dates Query... ' + time.strftime("%H:%M:%S"))
cur.execute(
			"insert into sandbox_andre_matias.aut_deals_insert_to_base_dates "\
				"select "\
					"'ptre' as source_system, "\
					"max(fac.dat_payment) "\
				"from "\
					"sandbox_andre_matias.t_fac_paidad_user_payment fac "\
				"where "\
					"cod_source_system = 3")
conn.commit()
print('Ending Dates Query... ' + time.strftime("%H:%M:%S"))

print('Starting Deals Creations in Base... ' + time.strftime("%H:%M:%S"))

# Threading implementation
number_active_threads = 0
number_deals = len(result_list)
deals_per_thread = - (-number_deals // MAX_ACTIVE_THREADS) # Ceiling of integer division
thread_list = []

i = 0
j = deals_per_thread
for n in range(0, MAX_ACTIVE_THREADS):
	t = threading.Thread(target=createDealsInBase, args=(client, result_list[i:j]))
	thread_list.append(t)
	t.start()
	print('Spawned thread #' + str(n+1))
	i = i + deals_per_thread
	j = j + deals_per_thread
	if j > number_deals:
		j = number_deals

for t in thread_list:
	t.join()
	
print('Ending Deals Creations in Base... ' + time.strftime("%H:%M:%S"))	

scai.processEnd(conf_file, scai_process_name, COD_INTEGRATION, COD_COUNTRY, '', '',1)	# SCAI
scai.integrationEnd(conf_file, COD_INTEGRATION, COD_COUNTRY, 1)		# SCAI	

cur.close()
conn.close()
	
print('Ending Process... ' + time.strftime("%H:%M:%S"))