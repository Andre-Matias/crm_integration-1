#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import csv, ast, psycopg2, json, basecrm, gzip
from munch import * 
import time
import aut_olxpt_base_to_bd_stages_stage_movement
import aut_olxpt_base_to_bd_deals_stage_movement
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
		deal.id = result[0]
		deal.stage_id = result[1]
		client.deals.update(deal.id, deal)
	
def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

conf_file = sys.argv[1] # File with source database

base_api_token = json.load(open(conf_file))['base_api_token_olxpt'] 

client = basecrm.Client(access_token=base_api_token)

conn = getDatabaseConnection(conf_file)
cur = conn.cursor()

print('Starting Query: ' + time.strftime("%H:%M:%S"))

cur.execute(
			"select "\
              "a.opr_deal, "\
              "case when and to_date(left(a.last_stage_change_at,10),'yyyy-mm-dd') < to_date(sysdate,'yyyy-mm-dd') - 10 then 7344248 "\ #Lost
			  "when to_date(left(c.last_call,10),'yyyy-mm-dd') < 4 then 7344247 "\ # Unqualified
			  "when to_date(left(c.last_call,10),'yyyy-mm-dd') > 4 then 7344248 "\ # Lost
			  "end as opr_stage, "\
              "to_date(left(last_stage_change_at,10),'yyyy-mm-dd') last_stage_change_at, "\
              "to_date(sysdate,'yyyy-mm-dd') - 10 today_minus_10, "\
			  "c.last_call, "\
              "b.name "\
            "from "\
              "sandbox_andre_matias.t_lkp_deal a, "\
              "sandbox_andre_matias.t_lkp_stage b "\
			  "( "\
			  "select "\
			  "	b.cod_deal, "\
			  "	max(a.updated_at) last_call	"\
			  "from "\
			  "	sandbox_andre_matias.t_fac_call a, "\
			  " sandbox_andre_matiast_fac_call_deal b "\
			  "where "\
			  " a.cod_call = b.cod_call "\
			  " a.cod_source_system = 16 "\
			  "group by "\
			  " b.cod_deal "\
			  ") c "\
            "where "\
			  "a.valid_to = 20991231 "\
			  "and a.cod_source_system = 16 "\
			  "a.cod_deal = c.cod_deal "\
              #"and to_date(left(a.last_stage_change_at,10),'yyyy-mm-dd') < to_date(sysdate,'yyyy-mm-dd') - 10 "\   DEPOIS DA PRIMEIRA EXECUÇÃO, ACTIVA-SE ESTE FILTRO!!!!!
              "and a.cod_stage = b.cod_stage "\
              "and b.dsc_stage not in('Lost','Won','Unqualified');")
result_list = cur.fetchall()

# 7344248 = Lost: Ver Pipeline do Stage
# 2950784 = Lost: Ver Pipeline do Stage
# 7344247 = Unqualified: Ver Pipeline do Stage
# 2950783 = Unqualified: Ver Pipeline do Stage

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