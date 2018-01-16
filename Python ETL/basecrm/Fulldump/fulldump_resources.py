import basecrm
import boto
from boto.s3.key import Key
from munch import *
import simplejson as json
from decimal import *
import os
import sys
import gzip
import dateutil.parser
from datetime import datetime
import requests
import time


def s3_fulldump_deals(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting deals data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/deals_"

	while 1:
		
		data = client.deals.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for deal_data in data:
			deal_data['meta_event_type'] = 'created'
			deal_data['meta_event_time'] = datetime.now().isoformat()
			deal_data['country'] = country
			deal_data['category'] = category
			output.write(json.dumps(deal_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"
		
		fileName="deals_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"deals/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1

def s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting contacts data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/contacts_"
	while 1:
		
		data = client.contacts.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for contact_data in data:
			contact_data['meta_event_type'] = 'created'
			contact_data['meta_event_time'] = datetime.now().isoformat()
			contact_data['country'] = country
			contact_data['category'] = category
			output.write(json.dumps(contact_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="contacts_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"contacts/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1

def s3_fulldump_leads(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting leads data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/leads_"
	while 1:
		
		data = client.leads.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for lead_data in data:
			lead_data['meta_event_type'] = 'created'
			lead_data['meta_event_time'] = datetime.now().isoformat()
			lead_data['country'] = country
			lead_data['category'] = category
			output.write(json.dumps(lead_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="leads_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"leads/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1

def s3_fulldump_users(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting users data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/users_"
	while 1:
		
		data = client.users.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for user_data in data:
			user_data['meta_event_type'] = 'created'
			user_data['meta_event_time'] = datetime.now().isoformat()
			user_data['country'] = country
			user_data['category'] = category
			output.write(json.dumps(user_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="users_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"users/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1

def s3_fulldump_stages(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting stages data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/stages_"
	while 1:
		
		data = client.stages.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for stage_data in data:
			stage_data['meta_event_type'] = 'created'
			stage_data['meta_event_time'] = datetime.now().isoformat()
			stage_data['country'] = country
			stage_data['category'] = category
			output.write(json.dumps(stage_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="stages_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"stages/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1

def s3_fulldump_loss_reasons(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting loss_reasons data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/loss_reasons_"
	while 1:
		
		data = client.loss_reasons.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for loss_reason_data in data:
			loss_reason_data['meta_event_type'] = 'created'
			loss_reason_data['meta_event_time'] = datetime.now().isoformat()
			loss_reason_data['country'] = country
			loss_reason_data['category'] = category
			output.write(json.dumps(loss_reason_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="loss_reasons_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"loss_reasons/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1		



def s3_fulldump_tasks(token,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting tasks data")
	
	aux = 1
	name = "/home/ubuntu/Reports/tasks_"
	while 1:

		url = "https://api.getbase.com/v2/tasks"
		response = requests.get(url,
			params={'per_page': 100,'page': aux},
			headers={'Authorization':'Bearer {}'.format(token)},timeout=2000)

		if response.status_code != 200:
	            raise Exception('Request failed with {}'
	                .format(response.status_code))
	            return 0

		data = response.json()['items']       

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		for tasks_data in data:
			tasks_data['meta_event_type'] = 'created'
			tasks_data['meta_event_time'] = datetime.now().isoformat()
			tasks_data['country'] = country
			tasks_data['category'] = category
			tasks_data['reminder_offset'] = 0
			output.write(json.dumps(tasks_data,use_decimal=True) + "\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="tasks_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"tasks/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)

		#Next page iterate
		aux += 1		



def s3_fulldump_notes(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting notes data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/notes_"
	while 1:
		
		data = client.notes.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for note_data in data:
			note_data['meta_event_type'] = 'created'
			note_data['meta_event_time'] = datetime.now().isoformat()
			note_data['country'] = country
			note_data['category'] = category
			output.write(json.dumps(note_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="notes_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"notes/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1			

def s3_fulldump_pipelines(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting pipelines data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/pipelines_"
	while 1:
		
		data = client.pipelines.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for pipeline_data in data:
			pipeline_data['meta_event_type'] = 'created'
			pipeline_data['meta_event_time'] = datetime.now().isoformat()
			pipeline_data['country'] = country
			pipeline_data['category'] = category
			output.write(json.dumps(pipeline_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="pipelines_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"pipelines/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1			

def s3_fulldump_sources(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting sources data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/sources_"
	while 1:
		
		data = client.sources.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for source_data in data:
			source_data['meta_event_type'] = 'created'
			source_data['meta_event_time'] = datetime.now().isoformat()
			source_data['country'] = country
			source_data['category'] = category
			output.write(json.dumps(source_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="sources_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"sources/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1	

def s3_fulldump_tags(client,keyId,sKeyId,bucketName,path,country,category):
	
	print("Getting tags data")
	#Iterate for everypage returned by the API
	aux = 1
	name = "/home/ubuntu/Reports/tags_"
	while 1:
		
		data = client.tags.list(page = aux, per_page = 100)

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		#Write on local gz file
		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		#Iterate the list of deals
		for tag_data in data:
			tag_data['meta_event_type'] = 'created'
			tag_data['meta_event_time'] = datetime.now().isoformat()
			tag_data['country'] = country
			tag_data['category'] = category
			output.write(json.dumps(tag_data,use_decimal=True) + "\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="tags_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"tags/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1							

def s3_fulldump_orders(token,keyId,sKeyId,bucketName,path,country,category):
	print("Getting orders data")
	
	aux = 1
	name = "orders_"
	name_line_items = "line_items_"
	while 1:

		url = "https://api.getbase.com/v2/orders"
		response = requests.get(url,
			params={'per_page': 100,'page': aux},
			headers={'Authorization':'Bearer {}'.format(token)})

		if response.status_code != 200:
	            raise Exception('Request failed with {}'
	                .format(response.status_code))
	            return 0

		data = response.json()['items']       

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')
		output_line_items = gzip.open(name_line_items + str(aux).zfill(10) + ".txt.gz", 'wb')

		for orders_data in data:
			orders_data['meta_event_type'] = 'created'
			orders_data['meta_event_time'] = datetime.now().isoformat()
			orders_data['country'] = country
			orders_data['category'] = category
			output.write(json.dumps(orders_data,use_decimal=True) + "\n")
			# Request the line items for this order_id
			get_order_line_items(orders_data['data']['id'],orders_data['data']['deal_id'],token,output_line_items,country,category)
		

		#Close gz file		
		output.close()
		output_line_items.close()

		#Upload file to S3
		localName = name + str(aux).zfill(10) + ".txt.gz"
		localName_line_items = name_line_items + str(aux).zfill(10) + ".txt.gz"

		fileName="orders_" + str(aux).zfill(10) + ".txt.gz"
		fileName_line_items="line_items_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"orders/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)


		full_key_name = os.path.join(path+"line_items/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName_line_items)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName_line_items)
		
		#Remove local gz file
		os.remove(localName)
		os.remove(localName_line_items)

		#Next page iterate
		aux += 1		

def get_order_line_items(order_id,deal_id,token,file,country,category):

	aux = 1
	while 1:

		url = "https://api.getbase.com/v2/orders/" + str(order_id) + "/line_items"
		response = requests.get(url,
			params={'per_page': 100,'page': aux},
			headers={'Authorization':'Bearer {}'.format(token)})

		if response.status_code != 200:
			# raise Exception('Request failed with {}'
			#     .format(response.status_code))
			count = 0
			while response.status_code != 200 or count <= 5:
				print('No response for ' + str(order_id) + '. Retrying...')
				time.sleep(5)
				response = requests.get(url,
					params={'per_page': 100,'page': aux},
					headers={'Authorization':'Bearer {}'.format(token)})
				count += 1

			if response.status_code != 200:
				raise Exception('Request failed with {}'
					.format(response.status_code))
				return 0

		data = response.json()['items']     

		if len(data) > 0: empty = False
		else:	return 1

		for line_items_data in data:
			line_items_data['data']['meta_event_type'] = 'created'
			line_items_data['data']['meta_event_time'] = datetime.now().isoformat()
			line_items_data['data']['country'] = country
			line_items_data['data']['category'] = category
			line_items_data['data']['order_id'] = order_id
			line_items_data['data']['deal_id'] = deal_id
			file.write(json.dumps(line_items_data,use_decimal=True) + "\n")
		
		#Next page iterate
		aux += 1		

def s3_fulldump_calls(token,keyId,sKeyId,bucketName,path,country,category):
	print("Getting calls data")
	
	aux = 1
	name = "calls_"

	# Calls API has a maximum pages of 1000 - https://developers.getbase.com/docs/rest/reference/private/calls
	while aux <= 1000:

		url = "https://api.getbase.com/v2_beta/calls"
		response = requests.get(url,
			params={'per_page': 100,'page': aux},
			headers={'Authorization':'Bearer {}'.format(token)})

		if response.status_code != 200:
	            raise Exception('Request failed with {}'
	                .format(response.status_code))
	            return 0

		data = response.json()['items']       

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		for calls_data in data:
			calls_data['meta_event_type'] = 'created'
			calls_data['meta_event_time'] = datetime.now().isoformat()
			calls_data['country'] = country
			calls_data['category'] = category
			output.write(json.dumps(calls_data,use_decimal=True) + "\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="calls_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"calls/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1		

def s3_fulldump_call_outcomes(token,keyId,sKeyId,bucketName,path,country,category):
	print("Getting call_outcomes data")
	
	aux = 1
	name = "/home/ubuntu/Reports/call_outcomes_"
	while 1:

		url = "https://api.getbase.com/v2_beta/call_outcomes"
		response = requests.get(url,
			params={'per_page': 100,'page': aux},
			headers={'Authorization':'Bearer {}'.format(token)})

		if response.status_code != 200:
	            raise Exception('Request failed with {}'
	                .format(response.status_code))
	            return 0

		data = response.json()['items']       

		if len(data) > 0: empty = False
		else:
			print("Uploaded #" + str(aux) + " files to S3") 
			return 1

		output = gzip.open(name + str(aux).zfill(10) + ".txt.gz", 'wb')

		for call_outcomes_data in data:
			call_outcomes_data['meta_event_type'] = 'created'
			call_outcomes_data['meta_event_time'] = datetime.now().isoformat()
			call_outcomes_data['country'] = country
			call_outcomes_data['category'] = category
			output.write(json.dumps(call_outcomes_data,use_decimal=True) + "\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
		localName = name + str(aux).zfill(10) + ".txt.gz"

		fileName="call_outcomes_" + str(aux).zfill(10) + ".txt.gz"

		full_key_name = os.path.join(path+"call_outcomes/"+str(datetime.now().strftime('%Y/%m/%d/')), fileName)
		conn = boto.connect_s3(keyId,sKeyId)
		bucket = conn.get_bucket(bucketName)
		k = bucket.new_key(full_key_name)
		k.key=full_key_name

		k.set_contents_from_filename(localName)
		
		#Remove local gz file
		os.remove(localName)
		
		#Next page iterate
		aux += 1		

 
def mapping_fulldump_methods(resource,access_token_base,keyId,sKeyId,bucketName,path,client,country,category):
	if 'deals' == resource: s3_fulldump_deals(client,keyId,sKeyId,bucketName,path,country,category)
	if 'contacts' == resource: s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path,country,category)
	if 'leads' == resource: s3_fulldump_leads(client,keyId,sKeyId,bucketName,path,country,category)
	if 'users' == resource: s3_fulldump_users(client,keyId,sKeyId,bucketName,path,country,category)
	if 'tags' == resource: s3_fulldump_tags(client,keyId,sKeyId,bucketName,path,country,category)
	if 'orders' == resource: s3_fulldump_orders(access_token_base,keyId,sKeyId,bucketName,path,country,category)
	if 'calls' == resource: s3_fulldump_calls(access_token_base,keyId,sKeyId,bucketName,path,country,category)
	if 'stages' == resource: s3_fulldump_stages(client,keyId,sKeyId,bucketName,path,country,category)
	if 'loss_reasons' == resource: s3_fulldump_loss_reasons(client,keyId,sKeyId,bucketName,path,country,category)
	if 'notes' == resource: s3_fulldump_notes(client,keyId,sKeyId,bucketName,path,country,category)
	if 'pipelines' == resource: s3_fulldump_pipelines(client,keyId,sKeyId,bucketName,path,country,category)
	if 'sources' == resource: s3_fulldump_sources(client,keyId,sKeyId,bucketName,path,country,category)
	if 'call_outcomes' == resource: s3_fulldump_call_outcomes(access_token_base,keyId,sKeyId,bucketName,path,country,category)
	if 'tasks' == resource: s3_fulldump_tasks(access_token_base,keyId,sKeyId,bucketName,path,country,category)


def mapping_fulldump_method_table(resource,access_token_base,keyId,sKeyId,bucketName,path,client,country,category):
	return {
	"deals":s3_fulldump_deals(client,keyId,sKeyId,bucketName,path,country,category),
	"contacts":s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path,country,category),
	"leads":s3_fulldump_leads(client,keyId,sKeyId,bucketName,path,country,category),
	"users":s3_fulldump_users(client,keyId,sKeyId,bucketName,path,country,category),
	"tags":s3_fulldump_tags(client,keyId,sKeyId,bucketName,path,country,category),
	"orders":s3_fulldump_orders(access_token_base,keyId,sKeyId,bucketName,path,country,category),
	"calls":s3_fulldump_calls(access_token_base,keyId,sKeyId,bucketName,path,country,category),
	"stages":s3_fulldump_stages(client,keyId,sKeyId,bucketName,path,country,category),
	"loss_reasons":s3_fulldump_loss_reasons(client,keyId,sKeyId,bucketName,path,country,category),
	"notes":s3_fulldump_notes(client,keyId,sKeyId,bucketName,path,country,category),
	"pipelines":s3_fulldump_pipelines(client,keyId,sKeyId,bucketName,path,country,category),
	"sources":s3_fulldump_sources(client,keyId,sKeyId,bucketName,path,country,category),
	"call_outcomes":s3_fulldump_call_outcomes(access_token_base,keyId,sKeyId,bucketName,path,country,category),
	"tasks":s3_fulldump_tasks(access_token_base,keyId,sKeyId,bucketName,path,country,category)
	}[resource]	


