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

#Get Slack token
try:
	cur.execute(
		" select token "\
		" from crm_integration_anlt.t_lkp_token "\
		" where 1 = 1 "\
		" and application = 'Slack' ")	
except Exception as e:  
	print (e)
	print (e.pgerror)
	sys.exit("The process aborted with error.")

slack_token = cur.fetchone()
	 
		
#Check if there are different cods for the same opr
cur.execute("select  count(*) from ( "\
			" select opr_atlas_user, cod_source_system,  count(distinct cod_atlas_user) "\
			" from crm_integration_anlt.t_lkp_atlas_user "\
			" where 1=1 "\
			" group by opr_atlas_user, cod_source_system "\
			" having count(distinct cod_atlas_user) >1  )"
		)
			
conn.commit()

results = cur.fetchone()
#result_list = cur.fetchall()
 

slack_text = "There are " + str(results[0]) + " opr with different cods on the table t_lkp_atlas_user. Please verify this problem!"  

response = slack.sendToSlack(slack_token, slack_text, "crm_integration_team")

if response["ok"]:
	print("Message posted successfully: " + response["message"]["ts"])
elif response["ok"] is False:
	print("Message not posted due to error: " + response["message"]["ts"]) 		
		
		
#Check if there are duplicates in t_lkp_deal
cur.execute("select  count(*) from ( "\
			" select opr_deal, valid_from , cod_source_system, count(*) "\
			" from crm_integration_anlt.t_lkp_deal "\
			" where valid_to = 20991231 "\
			" group by opr_deal, cod_source_system, valid_from "\
			" having count(*) > 1  )"
		)
			
conn.commit()

results = cur.fetchone()
#result_list = cur.fetchall()


slack_text = "There are " + str(results[0]) + " duplicates on the table t_lkp_deal. Please verify this problem!"  

response = slack.sendToSlack(slack_token, slack_text, "crm_integration_team")

if response["ok"]:
	print("Message posted successfully: " + response["message"]["ts"])
elif response["ok"] is False:
	print("Message not posted due to error: " + response["message"]["ts"]) 				


	
