import sys
from datetime import datetime
import psycopg2
import simplejson as json
import sys, os
from slackclient import SlackClient

def sendToSlack(slack_token, slack_message, slack_channel = "crm_integration"): 
	 
	sc = SlackClient(slack_token) 
	
	return sc.api_call(
	  "chat.postMessage",
	  channel = slack_channel,
	  text = slack_message,
	  username = "CRM_INTEGRATION bot"
	)
	


 