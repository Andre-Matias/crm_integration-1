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

# Convert timestamps
def convert_timestamps(data):
	for item in data:
			for k in item:
				if(k[-3:] == '_at' and type(item[k]) is unicode):
					item[k] = str(dateutil.parser.parse(item[k]))[:-6]
	return data				


def s3_fulldump_deals(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for deal_data in data:
			output.write(json.dumps(deal_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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



def s3_fulldump_contacts(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for contact_data in data:
			output.write(json.dumps(contact_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_leads(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for lead_data in data:
			output.write(json.dumps(lead_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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



def s3_fulldump_users(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for user_data in data:
			output.write(json.dumps(user_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_stages(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for stage_data in data:
			output.write(json.dumps(stage_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_loss_reasons(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for loss_reason_data in data:
			output.write(json.dumps(loss_reason_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_notes(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for note_data in data:
			output.write(json.dumps(note_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_pipelines(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for pipeline_data in data:
			output.write(json.dumps(pipeline_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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



def s3_fulldump_sources(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for source_data in data:
			output.write(json.dumps(source_data,use_decimal=True)+"\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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


def s3_fulldump_tags(client,keyId,sKeyId,bucketName,path):
	
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

		data = convert_timestamps(data)

		#Iterate the list of deals
		for tag_data in data:
			output.write(json.dumps(tag_data,use_decimal=True) + "\n")

		#Close gz file		
		output.close()

		#Upload file to S3
		str(datetime.now().strftime('%Y/%m/%d'))
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

