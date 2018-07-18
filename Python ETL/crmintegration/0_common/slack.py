import sys
from datetime import datetime
import psycopg2
import simplejson as json
import sys, os
from slackclient import SlackClient

def sendToSlack(slack_message, slack_channel = "crm_integration"): 
	
	slack_token =  "xoxp-8354699687-379113731443-400912298391-dfd11f9a103fe3261d8af394a6825bf8" 
	sc = SlackClient(slack_token) 
	
	return sc.api_call(
	  "chat.postMessage",
	  channel = slack_channel,
	  text = slack_message,
	  username = "CRM_INTEGRATION bot"
	)
	


 