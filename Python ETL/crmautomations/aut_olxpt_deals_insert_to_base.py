#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
import time
import threading

MAX_ACTIVE_THREADS = 5

def createDealsInBase(client, result_list):
	for result in result_list:
		keep_trying = True
		while keep_trying:
			try:
				deal = client.deals.create(
						name=result[0],
						contact_id=result[1],
						owner_id=result[2],
						source_id=result[3],
						stage_id=result[4],
						value=result[5], 
						custom_fields={'L2 Categoria':result[6],'L1 Categoria':result[7],'Payment Date':result[8]})
				break
			except basecrm.errors.ResourceError as err:
				print("Error: basecrm.errors.ResourceError: " + str(err) + "\nSkipping creation of deal for contact with ID " + str(result[1]))
				keep_trying = False
			except basecrm.errors.ServerError as err:
				print("Error: basecrm.errors.ServerError. Trying again...")

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

def main(conf_file):
	print('Starting Process... ' + time.strftime("%H:%M:%S"))
		
	base_api_token = json.load(open(conf_file))['base_api_token_olxpt'] 

	client = basecrm.Client(access_token=base_api_token)

	# Create Redshift Connection
	conn = getDatabaseConnection(conf_file)
	cur = conn.cursor()

	print('Starting Data Query... ' + time.strftime("%H:%M:%S"))
	cur.execute(
				"select "\
					"fac.dsc_paidad_user_payment,  "\
					"lkp_contact.opr_contact, "\
					"lkp_base_user.opr_base_user, "\
					"4616871 as opr_base_source, "\
					"case "\
						"when "\
								"v_lkp_pi.cod_index_type = 1 /* VAS */ "\
								"and to_date(fac.dat_valid_to,'yyyy-mm-dd') /* active package */ < to_date(sysdate,'yyyy-mm-dd') "\
								"and last_call < to_date(sysdate,'yyyy-mm-dd') -15 "\
							"then 7344246 /* organico */ "\
					"else 2950782 /* sales pipeline */ "\
					"end as opr_stage, "\
					"(fac.val_price * (-1))/1.23 as val_price, "\
					"lkp_category.dsc_category_en sub_category, "\
					"lkp_category_parent.dsc_category_en main_category, "\
					"to_char(to_date(fac.dat_payment,'yyyy/mm/dd'),'dd/mm/yyyy') as dat_payment "\
				"from "\
					"crm_integration_anlt.t_fac_paidad_user_payment fac, "\
					"crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user, "\
					"crm_integration_anlt.t_lkp_ad lkp_ad, "\
					"crm_integration_anlt.t_lkp_category lkp_category, "\
					"( "\
						"select cod_category, opr_category, dsc_category_en, parent_level1, parent_level2, cod_category_parent from crm_integration_anlt.t_lkp_category "\
						"where (cod_source_system = 8 "\
						"and valid_to = 20991231 "\
						"and parent_level2 is null "\
						"and parent_level1 is null) "\
						"or cod_source_system in(-1,-2) "\
					") lkp_category_parent, "\
					"crm_integration_anlt.t_lkp_contact lkp_contact, "\
					"crm_integration_anlt.t_lkp_base_user lkp_base_user, "\
					"crm_integration_anlt.t_lkp_paidad_index lkp_pi, "\
					"crm_integration_anlt.t_lkp_paidad_index_type lkp_pit, "\
					"(select cod_contact, to_date(max(updated_at),'yyyy-mm-dd') last_call from crm_integration_anlt.t_fac_call where cod_source_system = 16 group by cod_contact) fac_call "\
				"where "\
					"fac.cod_atlas_user = lkp_atlas_user.cod_atlas_user "\
					"and lkp_atlas_user.cod_source_system = 8 "\
					"and lkp_contact.cod_source_system = 16 "\
					"and lkp_atlas_user.cod_atlas_user = lkp_contact.cod_atlas_user "\
					"and fac.cod_paidad_index = lkp_pi.cod_paidad_index "\
					"and lkp_pi.cod_paidad_index_type = lkp_pit.cod_paidad_index_type "\
					"and lkp_pit.valid_to = 20991231 "\
					"and lkp_pit.cod_source_system = 8 "\
					"and lkp_pit.opr_paidad_index_type in ('ad_homepage','highlight','bundle','nnl','pushup','logo','topads','topupaccount','paid_subscription','paid_limits_single','paid_for_post') "\
					"and lkp_pi.cod_source_system = 8 "\
					"and fac.cod_ad = lkp_ad.cod_ad "\
					"and lkp_ad.valid_to = 20991231 "\
					"and lkp_ad.cod_source_system = 8 "\
					"and lkp_ad.cod_category = lkp_category.cod_category "\
					"and lkp_category.cod_source_system = 8 "\
					"and lkp_category.valid_to = 20991231 "\
					"and lkp_contact.cod_base_user_owner = lkp_base_user.cod_base_user "\
					"and lkp_base_user.cod_source_system = 16 "\
					"and lkp_base_user.valid_to = 20991231 "\
					"and isnull(lkp_category.parent_level1,-2) = lkp_category_parent.opr_category "\
					"and val_price < 0 "\
					"and dat_payment > (select last_processing_datetime from crm_integration_anlt.aut_deals_insert_to_base_date where source_system = 'pthorizontal') "\
					"and lkp_atlas_user.valid_to = 20991231 "\
					"and lkp_contact.valid_to = 20991231 "\
					"and lkp_pi.valid_to = 20991231 "\
					"and lkp_contact.cod_contact = fac_call.cod_contact (+); ")
	result_list = cur.fetchall()
	print('Ending Data Query... ' + time.strftime("%H:%M:%S"))

	print('Starting Delete Dates Query... ' + time.strftime("%H:%M:%S"))
	cur.execute(
				"delete from crm_integration_anlt.aut_deals_insert_to_base_date where source_system = 'pthorizontal'; ")
	print('Ending Delete Dates Query... ' + time.strftime("%H:%M:%S"))

	print('Starting Dates Query... ' + time.strftime("%H:%M:%S"))
	cur.execute(
				"insert into crm_integration_anlt.aut_deals_insert_to_base_date "\
					"select "\
						"'pthorizontal' as source_system, "\
						"max(fac.dat_payment) "\
					"from "\
					"crm_integration_anlt.t_fac_paidad_user_payment fac, "\
					"crm_integration_anlt.t_lkp_atlas_user lkp_atlas_user, "\
					"crm_integration_anlt.t_lkp_ad lkp_ad, "\
					"crm_integration_anlt.t_lkp_category lkp_category, "\
					"( "\
						"select cod_category, opr_category, dsc_category_en, parent_level1, parent_level2, cod_category_parent from crm_integration_anlt.t_lkp_category "\
						"where cod_source_system = 8 "\
						"and valid_to = 20991231 "\
						"and parent_level2 is null "\
						"and parent_level1 is null "\
						"union all "\
						"select cod_category, opr_category, dsc_category_en, parent_level1, parent_level2, cod_category_parent from crm_integration_anlt.t_lkp_category "\
						"where cod_category in (-1,-2) "\
					") lkp_category_parent, "\
					"crm_integration_anlt.t_lkp_contact lkp_contact, "\
					"crm_integration_anlt.t_lkp_base_user lkp_base_user, "\
					"crm_integration_anlt.t_lkp_paidad_index lkp_pi, "\
					"crm_integration_anlt.t_lkp_paidad_index_type lkp_pit "\
				"where "\
					"fac.cod_atlas_user = lkp_atlas_user.cod_atlas_user "\
					"and lkp_atlas_user.cod_source_system = 8 "\
					"and lkp_contact.cod_source_system = 16 "\
					"and lkp_atlas_user.cod_atlas_user = lkp_contact.cod_atlas_user "\
					"and fac.cod_paidad_index = lkp_pi.cod_paidad_index "\
					"and lkp_pi.cod_paidad_index_type = lkp_pit.cod_paidad_index_type "\
					"and lkp_pit.valid_to = 20991231 "\
					"and lkp_pit.cod_source_system = 8 "\
					"and lkp_pit.opr_paidad_index_type in ('bundle','nnl','pushup','logo','topads','topupaccount') "\
					"and lkp_pi.cod_source_system = 8 "\
					"and fac.cod_ad = lkp_ad.cod_ad "\
					"and lkp_ad.valid_to = 20991231 "\
					"and lkp_ad.cod_source_system = 8 "\
					"and lkp_ad.cod_category = lkp_category.cod_category "\
					"and lkp_category.cod_source_system = 8 "\
					"and lkp_category.valid_to = 20991231 "\
					"and lkp_contact.cod_base_user_owner = lkp_base_user.cod_base_user "\
					"and lkp_base_user.cod_source_system = 16 "\
					"and lkp_base_user.valid_to = 20991231 "\
					"and isnull(lkp_category.parent_level1,-2) = lkp_category_parent.opr_category "\
					"and val_price < 0 "\
					"and lkp_atlas_user.valid_to = 20991231 "\
					"and lkp_contact.valid_to = 20991231 "\
					"and lkp_pi.valid_to = 20991231; ")
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


	cur.close()
	conn.close()
					
	print('Ending Process... ' + time.strftime("%H:%M:%S"))

if __name__ == "__main__":
	conf_file = sys.argv[1] # File with source database
	main(conf_file)