import sys
from datetime import datetime
import psycopg2
import simplejson as json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '0_common'))  # Change this later to a package import
from slackclient import SlackClient
import slack

def getDatabaseConnection(conf_file):
	data = json.load(open(conf_file))
	return psycopg2.connect(dbname=data['dbname'], host=data['host'], port=data['port'], user=data['user'], password=data['pass'])

db_conf_file = sys.argv[1]	


conn = getDatabaseConnection(db_conf_file)
cur = conn.cursor()
	
#Get status and dates of last integrations per country
cur.execute("select "\
			" country_integration.cod_country, "\
			" country.dsc_country, "\
			" country_integration.dat_processing, "\
			" max(country_integration.cod_status) as cod_status, "\
			" case when status.dsc_status = 'Ok' then ':tada:' || status.dsc_status || ':tada:'   "\
			"      when status.dsc_status = 'Running' then ':runner:' || status.dsc_status || ':runner:'  "\
			"      when status.dsc_status = 'Error' then ':disappointed:' || status.dsc_status || ':disappointed:' end dsc_status, "\
			" min(execution_date) as min, "\
			" max(execution_date) as max, "\
			" max(execution_date)-min(execution_date)total_execution_time "\
			" from crm_integration_anlt.t_rel_scai_country_integration country_integration "\
			" 	left outer join crm_integration_anlt.t_fac_scai_execution fac on fac.dat_processing = country_integration.dat_processing and fac.cod_integration = country_integration.cod_integration and fac.cod_country = country_integration.cod_country, "\
			" crm_integration_anlt.t_lkp_country country, "\
			" crm_integration_anlt.t_lkp_scai_status status "\
			" where 1=1 "\
			" and country.cod_country = country_integration.cod_country "\
			" and country.valid_to = 20991231 "\
			" and country_integration.cod_status = status.cod_status "\
			" and country_integration.cod_integration in (10000,11000,30000) "\
			" group by "\
			" 	country_integration.cod_country, "\
			" 	country.dsc_country, "\
			" 	country_integration.dat_processing, "\
			"	status.dsc_status " 
		)
			
conn.commit()

#results = cur.fetchone()
result_list = cur.fetchall()

for results in result_list: 

	slack_text = "The integration for " + results[1] + " executed on the " + str(results[2]) + " having finished with " + results[4] + ". It started it's execution at " +  str(results[5])[0:19] + " and ended at " + str(results[6])[0:19] + ", and it took a total of " + str(results[7])[0:8]  

	response = test_slack.sendToSlack(slack_text, "crm_integration_team")
	
	if response["ok"]:
		print("Message posted successfully: " + response["message"]["ts"])
	elif response["ok"] is False:
		print("Message not posted due to error: " + response["message"]["ts"]) 


	
